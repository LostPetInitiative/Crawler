module Tracing

open System.Diagnostics

let private formatMessage source message = sprintf "%25s: %s" source message

let traceInfo source message = Trace.TraceInformation (formatMessage source message)
let traceWarning source message = Trace.TraceWarning (formatMessage source message)
let traceError source message = Trace.TraceError (formatMessage source message)
