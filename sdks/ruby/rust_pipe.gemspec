Gem::Specification.new do |s|
  s.name        = "rust_pipe"
  s.version     = "0.1.0"
  s.summary     = "Ruby worker SDK for rust-pipe"
  s.description = "Receive typed tasks from a Rust dispatcher over WebSocket"
  s.authors     = ["Albin"]
  s.license     = "MIT"
  s.files       = Dir["lib/**/*.rb"]

  s.required_ruby_version = ">= 3.1"
  s.add_dependency "websocket-client-simple", "~> 0.8"
  s.add_dependency "json", "~> 2.7"
end
