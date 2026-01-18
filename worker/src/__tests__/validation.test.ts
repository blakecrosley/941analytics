/**
 * Payload Validation Tests
 */
import { describe, it, expect } from "vitest";
import { validateCollectPayload, getDeviceType } from "../utils";

describe("validateCollectPayload", () => {
  describe("valid payloads", () => {
    it("accepts minimal valid payload", () => {
      const result = validateCollectPayload({
        site: "example.com",
        url: "https://example.com/page",
      });
      expect(result.valid).toBe(true);
      expect(result.error).toBeUndefined();
    });

    it("accepts full payload with all fields", () => {
      const result = validateCollectPayload({
        site: "example.com",
        url: "https://example.com/page",
        title: "My Page",
        ref: "https://google.com",
        w: 1920,
        h: 1080,
        sid: "abc123",
        type: "pageview",
      });
      expect(result.valid).toBe(true);
    });

    it("accepts event payload", () => {
      const result = validateCollectPayload({
        site: "example.com",
        url: "https://example.com/page",
        type: "event",
        event_type: "click",
        event_name: "cta_button",
        event_data: { position: "hero" },
      });
      expect(result.valid).toBe(true);
    });
  });

  describe("invalid payloads", () => {
    it("rejects null payload", () => {
      const result = validateCollectPayload(null);
      expect(result.valid).toBe(false);
      expect(result.error).toBe("Invalid payload");
    });

    it("rejects undefined payload", () => {
      const result = validateCollectPayload(undefined);
      expect(result.valid).toBe(false);
      expect(result.error).toBe("Invalid payload");
    });

    it("rejects non-object payload", () => {
      const result = validateCollectPayload("string");
      expect(result.valid).toBe(false);
      expect(result.error).toBe("Invalid payload");
    });

    it("rejects array payload", () => {
      const result = validateCollectPayload([]);
      expect(result.valid).toBe(false);
      expect(result.error).toBe("Invalid payload");
    });

    it("rejects missing site", () => {
      const result = validateCollectPayload({
        url: "https://example.com/page",
      });
      expect(result.valid).toBe(false);
      expect(result.error).toBe("Missing or invalid site");
    });

    it("rejects empty site", () => {
      const result = validateCollectPayload({
        site: "",
        url: "https://example.com/page",
      });
      expect(result.valid).toBe(false);
      expect(result.error).toBe("Missing or invalid site");
    });

    it("rejects non-string site", () => {
      const result = validateCollectPayload({
        site: 123,
        url: "https://example.com/page",
      });
      expect(result.valid).toBe(false);
      expect(result.error).toBe("Missing or invalid site");
    });

    it("rejects missing url", () => {
      const result = validateCollectPayload({
        site: "example.com",
      });
      expect(result.valid).toBe(false);
      expect(result.error).toBe("Missing or invalid url");
    });

    it("rejects empty url", () => {
      const result = validateCollectPayload({
        site: "example.com",
        url: "",
      });
      expect(result.valid).toBe(false);
      expect(result.error).toBe("Missing or invalid url");
    });

    it("rejects non-string url", () => {
      const result = validateCollectPayload({
        site: "example.com",
        url: 123,
      });
      expect(result.valid).toBe(false);
      expect(result.error).toBe("Missing or invalid url");
    });

    it("rejects invalid URL format", () => {
      const result = validateCollectPayload({
        site: "example.com",
        url: "not-a-valid-url",
      });
      expect(result.valid).toBe(false);
      expect(result.error).toBe("Invalid URL format");
    });
  });
});

describe("getDeviceType", () => {
  describe("device classification", () => {
    it("returns unknown for width 0", () => {
      expect(getDeviceType(0)).toBe("unknown");
    });

    it("returns mobile for small screens", () => {
      expect(getDeviceType(320)).toBe("mobile");
      expect(getDeviceType(375)).toBe("mobile");
      expect(getDeviceType(414)).toBe("mobile");
      expect(getDeviceType(767)).toBe("mobile");
    });

    it("returns tablet for medium screens", () => {
      expect(getDeviceType(768)).toBe("tablet");
      expect(getDeviceType(834)).toBe("tablet");
      expect(getDeviceType(1023)).toBe("tablet");
    });

    it("returns desktop for large screens", () => {
      expect(getDeviceType(1024)).toBe("desktop");
      expect(getDeviceType(1280)).toBe("desktop");
      expect(getDeviceType(1920)).toBe("desktop");
      expect(getDeviceType(2560)).toBe("desktop");
    });
  });

  describe("boundary conditions", () => {
    it("mobile/tablet boundary at 768", () => {
      expect(getDeviceType(767)).toBe("mobile");
      expect(getDeviceType(768)).toBe("tablet");
    });

    it("tablet/desktop boundary at 1024", () => {
      expect(getDeviceType(1023)).toBe("tablet");
      expect(getDeviceType(1024)).toBe("desktop");
    });
  });
});
