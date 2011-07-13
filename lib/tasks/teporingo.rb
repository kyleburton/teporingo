$teporingo_root = File.join(File.dirname(__FILE__),'..','..')
require File.dirname(__FILE__) + '/teporingo/rabbit'
require File.dirname(__FILE__) + '/teporingo/redis'
require File.dirname(__FILE__) + '/teporingo/haproxy'


class String

  def grey;       colorize(self, "\e[1m\e[30m"); end
  def red;        colorize(self, "\e[1m\e[31m"); end
  def green;      colorize(self, "\e[1m\e[32m"); end
  def brown;      colorize(self, "\e[1m\e[33m"); end
  def cyan;       colorize(self, "\e[1m\e[36m"); end
  def light_grey; colorize(self, "\e[0m\e[37m"); end
  def white;      colorize(self, "\e[1m\e[37m"); end
  def dark_green; colorize(self, "\e[32m");      end
  def yellow;     colorize(self, "\e[1m\e[33m"); end
  def blue;       colorize(self, "\e[1m\e[34m"); end
  def dark_blue;  colorize(self, "\e[34m");      end
  def purple;     colorize(self, "\e[1m\e[35m"); end
  def colorize(text, color_code)  "#{color_code}#{text}\e[0m" end
end

namespace :teporingo do
  desc "Run swank server"
  task :swank, :port do |t,args|
    port = args[:port] || '4005'
    Dir.chdir("teporingo") do |p|
      system "lein", "deps"
      system "lein", "swank", port
    end
  end

end

