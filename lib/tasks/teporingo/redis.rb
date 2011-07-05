namespace :teporingo do
  namespace :redis do
    desc "Download, build and Install Redis"
    task :install do
      system "bash", "-c", "cd redis;bash ./install.sh"
    end
  end
end
