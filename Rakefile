require File.dirname(__FILE__) + "/lib/tasks/teporingo"

namespace :teporingo do
  desc "Install"
  task :install do
    Dir.chdir "teporingo" do |p|
      unless system "lein", "install"
        raise "Error building teporingo!"
      end
    end
  end
end
