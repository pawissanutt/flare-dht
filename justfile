set windows-shell := ["pwsh.exe", "-NoLogo", "-Command"]
set export := true

build: 
    cargo build -r

run-leader RUST_LOG="INFO":
     wt -p "leader" -d . pwsh -c "cargo run -- server -p 8001 -n 1 -l"

run-cluster RUST_LOG="INFO":
     wt -p "leader" -d . pwsh -c "cargo run -- server -p 8001 -n 1 -l" `; split-pane -V -p "follower-1" -d . pwsh -c "cargo run -- server -p 8002 -n 2 --peer-addr http://127.0.0.1:8001" `; split-pane -H -p "follower-2" -d . pwsh -c "cargo run -- server -p 8003 -n 3 --peer-addr http://127.0.0.1:8001"

run-cluster-r RUST_LOG="INFO":
     wt -p "leader" -d . pwsh -c "cargo run -r -- server -p 8001 -n 1 -l" `; split-pane -V -p "follower-1" -d . pwsh -c "cargo run -r -- server -p 8002 -n 2 --peer-addr http://127.0.0.1:8001" `; split-pane -H -p "follower-2" -d . pwsh -c "cargo run -r -- server -p 8003 -n 3 --peer-addr http://127.0.0.1:8001"


run-grpcui:
     grpcui -plaintext -port 8080  localhost:8001

publish:
     cargo publish -p flare-pb
     cargo publish -p flare-zrpc
     cargo publish -p flare-dht
     cargo publish -p flare-cli