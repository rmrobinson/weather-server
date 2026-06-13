package solar

import (
	"math"
	"testing"
	"time"
)

// unixOf is a test helper that converts a UTC time to a Unix timestamp.
func unixOf(year, month, day, hour, min, sec int) int64 {
	t := time.Date(year, time.Month(month), day, hour, min, sec, 0, time.UTC)
	return t.Unix()
}

func TestCosSolarZenith_NoonAtEquatorOnEquinox(t *testing.T) {
	// 2024 March equinox: solar noon at lon=0, lat=0 → sun at zenith → cosZ ≈ 1.
	// March 20 2024 UTC solar noon at lon=0 is ~12:06 UTC.
	ts := unixOf(2024, 3, 20, 12, 6, 0)
	cosZ := CosSolarZenith(0, 0, ts)
	if math.Abs(cosZ-1.0) > 0.05 {
		t.Errorf("expected cosZ ≈ 1.0 at equatorial noon on equinox, got %.4f", cosZ)
	}
}

func TestCosSolarZenith_MidnightIsNegative(t *testing.T) {
	// Solar midnight at lon=0 → cosZ well below 0.
	ts := unixOf(2024, 6, 21, 0, 0, 0)
	cosZ := CosSolarZenith(0, 0, ts)
	if cosZ >= 0 {
		t.Errorf("expected negative cosZ at midnight, got %.4f", cosZ)
	}
}

func TestClearSkyGHI_ZeroAtNight(t *testing.T) {
	// Midnight UTC at lon=0, lat=0
	ts := unixOf(2024, 6, 21, 0, 0, 0)
	ghi := ClearSkyGHI(0, 0, ts)
	if ghi != 0 {
		t.Errorf("expected 0 W/m² at night, got %.2f", ghi)
	}
}

func TestClearSkyGHI_PositiveDuringDay(t *testing.T) {
	// Solar noon at lat=0, lon=0 on the summer solstice
	ts := unixOf(2024, 6, 21, 12, 0, 0)
	ghi := ClearSkyGHI(0, 0, ts)
	if ghi < 500 || ghi > 1000 {
		t.Errorf("expected 500–1000 W/m² at equatorial noon, got %.2f", ghi)
	}
}

func TestCloudCover_Nighttime(t *testing.T) {
	kt, pct := CloudCover(0, 0) // clearSkyGHI = 0 → nighttime
	if kt != 0 || pct != -1 {
		t.Errorf("nighttime: expected kt=0 pct=-1, got kt=%.2f pct=%.2f", kt, pct)
	}
}

func TestCloudCover_Clear(t *testing.T) {
	// Kt = 0.9 → clear sky → cloud cover in 0–20% range
	kt, pct := CloudCover(900, 1000)
	if math.Abs(kt-0.9) > 0.001 {
		t.Errorf("Kt: expected 0.9, got %.4f", kt)
	}
	if pct < 0 || pct > 20 {
		t.Errorf("clear sky: cloud cover should be 0–20%%, got %.1f%%", pct)
	}
}

func TestCloudCover_PartlyCloudy(t *testing.T) {
	// Kt = 0.55 → partly cloudy → 20–70%
	_, pct := CloudCover(550, 1000)
	if pct < 20 || pct > 70 {
		t.Errorf("partly cloudy: cloud cover should be 20–70%%, got %.1f%%", pct)
	}
}

func TestCloudCover_Overcast(t *testing.T) {
	// Kt = 0.1 → overcast → 70–100%
	_, pct := CloudCover(100, 1000)
	if pct < 70 || pct > 100 {
		t.Errorf("overcast: cloud cover should be 70–100%%, got %.1f%%", pct)
	}
}

func TestCloudCover_KtClamped(t *testing.T) {
	// Measured > clear-sky (calibration error or dust reflection) → Kt clamped to 1
	kt, _ := CloudCover(1200, 1000)
	if kt != 1.0 {
		t.Errorf("Kt should be clamped to 1.0, got %.4f", kt)
	}
}

func TestCloudCover_Boundaries(t *testing.T) {
	// At Kt=0.8 exactly: should be at the top of partly-cloudy / bottom of clear
	_, pct := CloudCover(800, 1000)
	if math.Abs(pct-20.0) > 0.5 {
		t.Errorf("Kt=0.8 boundary: expected ~20%% cloud, got %.1f%%", pct)
	}
	// At Kt=0.3 exactly: should be at the top of overcast / bottom of partly-cloudy
	_, pct = CloudCover(300, 1000)
	if math.Abs(pct-70.0) > 0.5 {
		t.Errorf("Kt=0.3 boundary: expected ~70%% cloud, got %.1f%%", pct)
	}
}
