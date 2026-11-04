import Long from "long";
import { google } from "../protos";

export function toDate(ts: google.protobuf.ITimestamp | string | any): Date {
  if (typeof ts === "string") {
    return new Date(ts);
  }

  if (ts && ts.seconds != null) {
    var seconds: number;
    if (ts.seconds instanceof Long) {
      seconds = ts.seconds.low;
    } else {
      seconds = ts.seconds;
    }
    return new Date(seconds * 1000);
  }

  return new Date(NaN);
}
