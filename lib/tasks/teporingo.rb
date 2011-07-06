$teporingo_root = File.join(File.dirname(__FILE__),'..','..')
require File.dirname(__FILE__) + '/teporingo/rabbit'
require File.dirname(__FILE__) + '/teporingo/redis'
require File.dirname(__FILE__) + '/teporingo/haproxy'

namespace :teporingo do
  desc "Run swank server"
  task :swank do
    Dir.chdir("rabbit-client") do |p|
      system "lein", "deps"
      system "lein", "swank"
    end
  end

end
