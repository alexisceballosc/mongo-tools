import { test, expect } from "bun:test";
import { GoBackError } from "./errors.js";

test("GoBackError is an instance of Error", () => {
  expect(new GoBackError()).toBeInstanceOf(Error);
});

test("GoBackError has message 'back'", () => {
  expect(new GoBackError().message).toBe("back");
});

test("GoBackError can be caught as a GoBackError", () => {
  const fn = () => { throw new GoBackError(); };
  expect(fn).toThrow(GoBackError);
});

test("GoBackError can be distinguished from other errors", () => {
  const err = new GoBackError();
  expect(err instanceof GoBackError).toBe(true);
  expect(new Error("other") instanceof GoBackError).toBe(false);
});
