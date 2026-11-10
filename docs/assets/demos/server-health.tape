# Native uc panes: control a real npm API and inspect its health and errors.
Output docs/assets/demos/server-health.gif
Require uc
Require npm
Require node
Require curl
Require python3
Source docs/assets/demos/theme.tape
Set Width 3200
Set Height 1200
Set FontSize 20
Source docs/assets/demos/setup.tape

Hide
Type@1ms "uc attach commands"
Enter
Wait+Screen /(?m)^uc \$( *$| {2,})/
Show
Type "# Service health comes from the API's health endpoint"
Enter
Type `uc start api --healthy-when "http:$APP/health" -- npm start`
Enter
Wait+Screen /(?m)^uc \$( *$| {2,})/
Type "# Alt-U then %: open the server in a second pane"
Enter
Alt+u
Type@100ms "%"
Wait+Screen /Split · Enter/
Sleep 1s
Enter
Wait+Screen /API listening on/
Alt+u
Type@100ms "o"
Sleep 500ms
Type "uc wait api --health healthy --timeout 15s"
Enter
Wait+Screen /(?m)^uc \$( *$| {2,})/
Type `curl -sS "$APP/orders"`
Enter
Wait+Screen /A100/
Wait+Screen /(?m)^uc \$( *$| {2,})/
Hide
Type@1ms "python3 server_check.py healthy || exit 1"
Enter
Wait+Screen /(?m)^uc \$( *$| {2,})/
Ctrl+l
Sleep 200ms
Show
Type "# Ready to serve requests"
Enter
Type "uc ls"
Enter
Wait+Screen /(?m)^api +running +[^\r\n]* +healthy +/
Wait+Screen /(?m)^uc \$( *$| {2,})/
Sleep 3s

Ctrl+l
Type "# One request fails; the service keeps working"
Enter
Type `curl -sS "$APP/orders?demo_error=1"`
Enter
Wait+Screen /simulated request failure/
Wait+Screen /(?m)^uc \$( *$| {2,})/
Type `curl -sS "$APP/orders"`
Enter
Wait+Screen /A100/
Wait+Screen /(?m)^uc \$( *$| {2,})/
Hide
Type@1ms "python3 server_check.py request-error || exit 1"
Enter
Wait+Screen /(?m)^uc \$( *$| {2,})/
Ctrl+l
Sleep 200ms
Show
Type "# A request error in the logs; the service is healthy"
Enter
Type "uc ls"
Enter
Wait+Screen /(?m)^api +running +[^\r\n]* +healthy +/
Wait+Screen /(?m)^uc \$( *$| {2,})/
Type "uc logs api --max-bytes 4096"
Enter
Wait+Screen /(?m)^uc \$( *$| {2,})/
Sleep 5s

Ctrl+l
Type "# Now disable an essential dependency in the sample app"
Enter
Type `curl -sS -X POST "$APP/demo/fail"`
Enter
Wait+Screen /(?m)^uc \$( *$| {2,})/
Type `curl -sS "$APP/orders"`
Enter
Wait+Screen /\[ERROR\] GET \/orders -> 503/
Wait+Screen /(?m)^uc \$( *$| {2,})/
Type "uc wait api --health unhealthy --timeout 15s"
Enter
Wait+Screen /(?m)^uc \$( *$| {2,})/
Hide
Type@1ms "python3 server_check.py unhealthy || exit 1"
Enter
Wait+Screen /(?m)^uc \$( *$| {2,})/
Ctrl+l
Sleep 200ms
Show
Type "# Still running, but unable to serve orders"
Enter
Type "uc ls"
Enter
Wait+Screen /(?m)^api +running +[^\r\n]* +unhealthy +/
Wait+Screen /(?m)^uc \$( *$| {2,})/
Sleep 4s

Ctrl+l
Type "# Restore service; old errors remain in the logs"
Enter
Type `curl -sS -X POST "$APP/demo/recover"`
Enter
Wait+Screen /(?m)^uc \$( *$| {2,})/
Type "uc wait api --health healthy --timeout 15s"
Enter
Wait+Screen /(?m)^uc \$( *$| {2,})/
Type `curl -sS "$APP/orders"`
Enter
Wait+Screen /A100/
Wait+Screen /(?m)^uc \$( *$| {2,})/
Hide
Type@1ms "python3 server_check.py recovered || exit 1"
Enter
Wait+Screen /(?m)^uc \$( *$| {2,})/
Ctrl+l
Sleep 200ms
Show
Type "# Healthy again; earlier errors are still available"
Enter
Type "uc ls"
Enter
Wait+Screen /(?m)^api +running +[^\r\n]* +healthy +/
Wait+Screen /(?m)^uc \$( *$| {2,})/
Type "uc logs api --max-bytes 4096"
Enter
Wait+Screen /(?m)^uc \$( *$| {2,})/
Sleep 5s
Screenshot docs/assets/demos/server-health-poster.png
Sleep 500ms
Type "uc stop api"
Enter
Wait+Screen /(?m)^uc \$( *$| {2,})/
Sleep 2s
Hide
Alt+u
Type@100ms "d"
Wait+Line /demo \$$/
Type@1ms "uc stop commands"
Enter
Wait+Line /demo \$$/
Type@1ms "touch recording-complete"
Enter
Wait+Line /demo \$$/
