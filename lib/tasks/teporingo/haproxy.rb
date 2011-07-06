$haproxy_config = YAML.load_file(File.join($teporingo_root,'config','haproxy.yml'))

def haproxy_download_url
  $haproxy_config["haproxy"]["download_url"]
end

def haproxy_file_name
  haproxy_download_url.split('/')[-1]
end

def haproxy_dir
  File.basename(haproxy_file_name,".tar.gz")
end

def haproxy_version
  haproxy_dir.split('-')[-1]
end

namespace :teporingo do
  namespace :haproxy do
    desc "Install Haproxy"
    task :install do
      Dir.mkdir "haproxy" unless File.exist? "haproxy"
      Dir.chdir "haproxy" do |p|
        unless File.exist? haproxy_file_name
          system "wget", haproxy_download_url
        end

        unless File.exist? haproxy_dir
          system "tar", "xzvf", haproxy_file_name
        end

        unless File.exist? "#{haproxy_dir}/haproxy"
          Dir.chdir(haproxy_dir) do |p|
            system "make", "TARGET=generic USE_KQUEUE=1 USE_POLL=1 USE_PCRE=1"
          end
        end
      end
    end

    desc "run haproxy"
    task :run => [:install] do
      cmd = %W|haproxy/#{haproxy_dir}/haproxy -V -db -f config/haproxy/haproxy-amqp.conf|
      puts cmd.inspect
      system *cmd
    end
  end
end
