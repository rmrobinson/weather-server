package grpcserver

import (
	"context"
	"crypto/subtle"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/rmrobinson/weather-server/internal/hub"
	"github.com/rmrobinson/weather-server/internal/store"
	"github.com/rmrobinson/weather-server/internal/types"
	weatherv1 "github.com/rmrobinson/weather-server/proto/weather/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Server struct {
	weatherv1.UnimplementedWeatherServiceServer
	hub    *hub.Hub
	store  *store.Store
	logger *zap.Logger
}

func New(h *hub.Hub, st *store.Store, logger *zap.Logger) *Server {
	return &Server{hub: h, store: st, logger: logger}
}

func conditionToProto(c string) weatherv1.WeatherCondition {
	switch c {
	case "Sunny":         return weatherv1.WeatherCondition_WEATHER_CONDITION_SUNNY
	case "Mostly Sunny":  return weatherv1.WeatherCondition_WEATHER_CONDITION_MOSTLY_SUNNY
	case "Partly Cloudy": return weatherv1.WeatherCondition_WEATHER_CONDITION_PARTLY_CLOUDY
	case "Mostly Cloudy": return weatherv1.WeatherCondition_WEATHER_CONDITION_MOSTLY_CLOUDY
	case "Overcast":      return weatherv1.WeatherCondition_WEATHER_CONDITION_OVERCAST
	case "Light Rain":    return weatherv1.WeatherCondition_WEATHER_CONDITION_LIGHT_RAIN
	case "Rain":          return weatherv1.WeatherCondition_WEATHER_CONDITION_RAIN
	case "Heavy Rain":    return weatherv1.WeatherCondition_WEATHER_CONDITION_HEAVY_RAIN
	case "Freezing Rain": return weatherv1.WeatherCondition_WEATHER_CONDITION_FREEZING_RAIN
	case "Snow":          return weatherv1.WeatherCondition_WEATHER_CONDITION_SNOW
	case "Night":         return weatherv1.WeatherCondition_WEATHER_CONDITION_NIGHT
	default:              return weatherv1.WeatherCondition_WEATHER_CONDITION_UNSPECIFIED
	}
}

func toProto(r types.WeatherReading) *weatherv1.WeatherReading {
	return &weatherv1.WeatherReading{
		Timestamp: timestamppb.New(r.Timestamp),
		// Outdoor
		TempC:       r.TempC,
		HumidityPct: r.HumidityPct,
		// Indoor
		TempInC:      r.TempInC,
		HumidityInPct: r.HumidityInPct,
		// Pressure
		PressureHpa:    r.PressureHPa,
		PressureAbsHpa: r.PressureAbsHPa,
		// Wind
		WindSpeedMs:    r.WindSpeedMs,
		WindGustMs:     r.WindGustMs,
		MaxDailyGustMs: r.MaxDailyGustMs,
		WindDirDeg:     r.WindDirDeg,
		// Rain
		RainMmHr:      r.RainMmHr,
		RainEventMm:   r.RainEventMm,
		RainHourlyMm:  r.RainHourlyMm,
		RainDailyMm:   r.RainDailyMm,
		RainWeeklyMm:  r.RainWeeklyMm,
		RainMonthlyMm: r.RainMonthlyMm,
		RainYearlyMm:  r.RainYearlyMm,
		// Derived atmospheric
		DewPointC: r.DewPointC,
		// Solar / UV
		UvIndex:   r.UVIndex,
		SolarWm2:     r.SolarWm2,
		ClearSkyWm2:  r.ClearSkyWm2,
		ClearSkyIndex: r.ClearSkyIdx,
		CloudCoverPct: r.CloudCovPct,
		// Sensor health
		BatteryV:   r.BatteryV,
		CapacitorV: r.CapacitorV,
		// Derived situational
		FeelsLikeC: r.FeelsLikeC,
		Condition:  conditionToProto(r.Condition),
	}
}

func (s *Server) StreamReadings(_ *weatherv1.StreamRequest, stream weatherv1.WeatherService_StreamReadingsServer) error {
	id := uuid.New().String()
	// Subscribe before querying latest so we don't miss a reading that arrives
	// between the query and entering the loop.
	sub := s.hub.Subscribe(id)
	defer s.hub.Unsubscribe(id)

	// Send the most recent stored reading immediately so the client doesn't
	// have to wait up to 60 seconds for the next MQTT cycle.
	if latest, err := s.store.QueryLatest(stream.Context()); err != nil {
		s.logger.Warn("could not fetch latest reading for stream init", zap.Error(err))
	} else if latest != nil {
		if err := stream.Send(toProto(*latest)); err != nil {
			return err
		}
	}

	for {
		select {
		case r := <-sub.Ch:
			if err := stream.Send(toProto(r)); err != nil {
				s.logger.Debug("stream send failed", zap.String("subscriber", id), zap.Error(err))
				return err
			}
		case <-stream.Context().Done():
			return nil
		}
	}
}

func (s *Server) QueryRainAccumulation(ctx context.Context, req *weatherv1.RainAccumulationRequest) (*weatherv1.RainAccumulationResponse, error) {
	if req.Start == nil {
		return nil, status.Error(codes.InvalidArgument, "start is required")
	}
	start := req.Start.AsTime()
	// End is optional; zero time.Time signals the store to default to now.
	var endTime time.Time
	if req.End != nil {
		endTime = req.End.AsTime()
	}

	result, err := s.store.QueryRainAccumulation(ctx, start, endTime)
	if err != nil {
		if errors.Is(err, store.ErrRainReset) {
			return nil, status.Error(codes.OutOfRange, err.Error())
		}
		s.logger.Error("query rain accumulation", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "query failed: %v", err)
	}
	if result == nil {
		return nil, status.Error(codes.NotFound, "no data in requested range")
	}
	return &weatherv1.RainAccumulationResponse{
		RainMm:      result.RainMm,
		ActualStart: timestamppb.New(result.ActualStart),
		ActualEnd:   timestamppb.New(result.ActualEnd),
	}, nil
}

func pskCheck(psk string, md metadata.MD) error {
	if psk == "" {
		return nil
	}
	vals := md.Get("authorization")
	want := []byte("psk " + psk)
	var got []byte
	if len(vals) > 0 {
		got = []byte(vals[0])
	}
	if subtle.ConstantTimeCompare(got, want) != 1 {
		return status.Error(codes.Unauthenticated, "invalid PSK")
	}
	return nil
}

func PSKStreamInterceptor(psk string) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if psk == "" {
			return handler(srv, ss)
		}
		md, ok := metadata.FromIncomingContext(ss.Context())
		if !ok {
			return status.Error(codes.Unauthenticated, "missing metadata")
		}
		if err := pskCheck(psk, md); err != nil {
			return err
		}
		return handler(srv, ss)
	}
}

func PSKUnaryInterceptor(psk string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if psk == "" {
			return handler(ctx, req)
		}
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return nil, status.Error(codes.Unauthenticated, "missing metadata")
		}
		if err := pskCheck(psk, md); err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

func NewGRPCServer(psk string) *grpc.Server {
	return grpc.NewServer(
		grpc.StreamInterceptor(PSKStreamInterceptor(psk)),
		grpc.UnaryInterceptor(PSKUnaryInterceptor(psk)),
	)
}
