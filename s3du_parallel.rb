#!/bin/env ruby
require 'base64'
require 'net/http'
require 'openssl'
require 'time'
require 'rexml/document'
require 'uri'
require 'thread'
require 'rubygems'
require 'parallel'

ENDPOINT = 's3.amazonaws.com'

Net::HTTP.version_1_2

class S3
  @access_key_id = nil
  @secret_access_key = nil

  def initialize
    f = open("#{ENV["HOME"]}/.s3cfg")
    while line = f.gets
      if line =~ /^access_key/ then
        access_key_id = line.split('=')[1].strip
      elsif line =~ /^secret_key/ then
        secret_access_key = line.split('=')[1].strip
      end
    end
    f.close
    if access_key_id.nil? or secret_access_key.nil? then
      puts "access_key_id or secret_access_key is not exist."
      exit 1
    end
    @access_key_id = access_key_id
    @secret_access_key = secret_access_key
  end

  def rest(bucket, path)
    host = bucket + "." + ENDPOINT  # mybucket.s3.amazonous.com
    date = Time.now.rfc2822
    spath = path.gsub(/\?.*/,'')  # remove parameter(?max-keys=n&marker=xx)
    string_to_sign = "GET\n\n\n#{date}\n/#{bucket}#{spath}"
    digest = OpenSSL::HMAC.digest(OpenSSL::Digest::SHA1.new, @secret_access_key, string_to_sign)
    signature = Base64.encode64(digest).gsub("\n", '')
  
    header = {
      'Host' => host,
      'Date' => date,
      'Authorization' => "AWS #{@access_key_id}:#{signature}"
    }

    content = nil

    Net::HTTP.start(host, 80) do |http|
      content = http.get(path, header).body
    end

    return content
  end
end

def directory(bucket ,prefix)
  obj = Array.new
  s3 = S3.new

  path = "/?prefix=#{prefix}&delimiter=/"
  res = s3.rest(bucket, path)

  xml= REXML::Document.new(res)
  xml.root.each_element do |element|
    meta = Hash.new
    if !element.elements["Prefix"].nil? then
      if element.elements["Prefix"] != prefix then
        meta['Prefix'] = element.elements["Prefix"].text
        obj << meta
      end
    end
  end
  return obj
end


def tree(bucket, subdir, depth, max_depth, paths)
  depth = depth + 1
  directory( bucket, subdir).each do |obj|
    paths << obj['Prefix']
    if depth != max_depth then
      tree(bucket, obj['Prefix'], depth, max_depth, paths)
    end
  end
end

def objects(bucket, prefix, marker, max_keys, max_depth)
  obj = Array.new
  s3 = S3.new

  if prefix.split("/").size == max_depth then
    path = "/?prefix=#{prefix}&marker=#{marker}&max-keys=#{max_keys}"
  else
    path = "/?prefix=#{prefix}&marker=#{marker}&max-keys=#{max_keys}&delimiter=/"
  end
  res = s3.rest(bucket, path)

  xml= REXML::Document.new(res)
  xml.root.each_element do |element|
    meta = Hash.new
    if !element.elements["Key"].nil? then
      meta['key'] = element.elements["Key"].text
    end
    if !element.elements["Size"].nil? then
      meta['size'] = element.elements["Size"].text.to_i
      obj << meta
    end
  end
  return obj
end

def get_size( bucket, path, max_depth )
  size = 0
  i = 0
  marker = nil
  lastkey = nil

  while true do
    objects( bucket, path, marker, 500, max_depth).each do |obj|
      size = size + obj['size'].to_i
      i = i + 1
      lastkey = obj['key']
    end
    if marker == lastkey
      break
    end
    marker = lastkey
  end

  result = Hash.new
  result['files'] = i
  result['size'] = size
  return result
end

#----------- main ------------

if ARGV[0].nil? then
  puts "usage: s3du.rb bucketname [max_depth]"
  exit 0
end

depth = 0

if !ARGV[1].nil? && ARGV[1].to_i > 0 then
  max_depth = ARGV[1].to_i
else
  max_depth = 2
end

paths = Array.new

tree(ARGV[0], "", depth, max_depth, paths)

all_files = 0
all_size = 0

paths << ""   # for root path

all_info = Parallel.map(paths){|path|
  ret = Hash.new
  ret = get_size( ARGV[0], path, max_depth)
  printf("path /%s\n", path)
  printf("total %d files\n", ret['files'])
  printf("usage %d bytes\n", ret['size'])
  STDOUT.flush
  ret
}

all_info.each do |ret|
  all_files = all_files + ret['files']
  all_size = all_size + ret['size']
end

puts "------- Overall ----------------------"
printf("total %d files\n", all_files)
printf("usage %d bytes\n", all_size)
  
exit 0
