#!/bin/ruby

CHUNK_SIZE = 100*1024*1024 # 100MB

require 'open3'
require 'json'
require 'securerandom'
require 'base64'

## fun
def rand(len=16)
  SecureRandom.bytes(16)
end
def kdf(input, iterations=10)
  salt = rand
  {
    hash: OpenSSL::KDF.pbkdf2_hmac(input, salt:salt, iterations: iterations, length: 32, hash: "sha256").each_byte.map{|s|s.to_s(16)}.join,
    salt: salt
  }
end
def parse_server(str)
  case str
  when /:\/\//
    require 'uri'
    a = URI.parse(str)
    raise "unknown protocol" unless a.scheme == 'ssh'
    [a.hostname, a.port || 22]
  when /\A([\w\d.-]+)(?::(\d+))?\z/
    [$1, ($2 || 22).to_i]
  else
    raise 'Can\'t parse server'
  end
end
def decrypt(str, pw)
  # do not forget pw stretching
end
def encrypt(str, pw)
  # do not forget pw stretching
end
def encrypt_for_pubkey(str, pubkey)
end
def encrypt_data(str, pubkey)
  key = rand()
  Base64.encode64(encrypt_for_pubkey(key, pubkey)) + "\n" + encrypt(str, key)
end
def metadata_parse(str, pw)
  JSON.parse(decrypt(str, pw))
end


# args!
if ARGV.length != 2
  puts "#{$PROGRAM_NAME}: <clusternode.example> <file/to.upload>"
  exit 1
end
raise "#{ARGV[1]} not readable" unless File.readable?(ARGV[1])
FILE = File.open(ARGV[1], mode: 'rb')

SERVER = parse_server(ARGV[0])
# print "Cluster PW:"
# CLUSTER_PW = STDIN.gets.chomp

INTERNAL_PORT = ENV.fetch("PORT", 1234).to_i


## GOGOGO
# stdin, stdout, stderr, _ = Open3.popen3("ssh -W localhost:#{INTERNAL_PORT} #{SERVER.join(':')}")
# stdin.puts("metadata-general get")
# metadata = metadata_parse(stdout.read(), CLUSTER_PW)
# stdin.puts("free-space get")
# space = stdout.read()

metadata = {"server"=>[["1",1*1024*1024*1024],["2",2*1024*1024*1024]]}
# metadata_enc, _err = Open3.capture3("ssh -W localhost:#{INTERNAL_PORT} #{SERVER.join(':')} mzfs.rb metadata-general get")
# metadata = metadata_parse(metadata_enc, CLUSTER_PW)
# metadata["server"].map! do |server|
#   server = parse_server(server)
#   space, _err = Open3.capture3("ssh -W localhost:#{INTERNAL_PORT} #{server.join(':')} mzfs.rb free-space get")
#   [server, space.to_i]
# end

metadata["server"].reject!{|name, free_space| free_space <= CHUNK_SIZE}
raise "not enough space left!" unless metadata["server"].map(&:last).sum >= FILE.size
metadata["server"] = Hash[metadata["server"]]

q = SizedQueue.new(2)
hashes = []

producer = Thread.new do
  i = 0;
  while chunk = FILE.read(CHUNK_SIZE)
    # q.push(encrypt_data(chunk, metadata["pubkey"]))
    server, free_space = metadata["server"].to_a.sample
    metadata["server"][server] = free_space - chunk.size
    q.push([chunk, server])
    puts "chunk #{i} produced, server #{server} should have #{metadata["server"][server]}b left"
    i += 1
  end
  q.close
end

consumers = []
2.times do |consumer_id|
  consumers << Thread.new do
    i = 0;
    while arr = q.pop
      chunk, server = arr
      puts "Consumer#{consumer_id}: sending chunk of size: #{chunk.size}b to #{server}"
      writer_hash = kdf(chunk)
      # server_hash, stderr = Open3.capture3("ssh -W localhost:#{INTERNAL_PORT} #{server}} mzfs.rb data write #{chunk.size}", stdin_data: chunk)
      # raise "non empty stderr" unless stderr
      # raise "upload failed" unless server_hash == writer_hash
      hashes << writer_hash
      sleep Kernel.rand
      puts "Consumer#{consumer_id}: #{i}th chunk consumed"
      i += 1
    end
  end
end

consumers.each{|thread|thread.join}
puts "done"
p hashes

# todo write metadata
