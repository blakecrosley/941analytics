import { describe, it, expect } from "vitest";
import { utcDayBounds } from "../utils";

describe("utcDayBounds", () => {
  it("returns the half-open SQLite datetime range of one UTC day", () => {
    expect(utcDayBounds("2026-10-29")).toEqual({ start: "2026-10-29 00:00:00", end: "2026-10-30 00:00:00" });
  });

  it("rolls over month, year and leap day", () => {
    expect(utcDayBounds("2026-10-31").end).toBe("2026-11-01 00:00:00");
    expect(utcDayBounds("2026-12-31").end).toBe("2027-01-01 00:00:00");
    expect(utcDayBounds("2028-02-29").end).toBe("2028-03-01 00:00:00");
  });

  it("rejects anything that is not a calendar date", () => {
    for (const bad of ["", "2026-13-40", "2026-02-30", "2026-1-5", "yesterday", "2026-10-29 00:00:00"]) {
      expect(() => utcDayBounds(bad)).toThrow();
    }
  });
});
