require 'yaml'

$rabbit_config = YAML.load_file(File.join($teporingo_root,'config','rabbitmq.yml'))["rabbit"]

def nodename node
  "#{node["name"]}@#{node["host"]}"
end

def rabbitmqctl node, action, *fields
  name = nodename node
  cmd = "rabbitmqctl", "-q", "-n", name, action, *fields
  puts cmd.join(" ")
  if fields.first == '-p'
    fields.shift
    fields.shift
  end
  puts fields.join("\t") unless fields.nil?
  system *cmd
  puts ""
end

namespace :teporingo do
  namespace :rabbit do
    desc "Install Rabbitmq locally"
    task :install do
      Dir.chdir "rabbitmq-server"
      rabbit_url = "http://www.rabbitmq.com/releases/rabbitmq-server/v2.5.1/rabbitmq-server-generic-unix-2.5.1.tar.gz"
      rabbit_archive = rabbit_url.split('/')[-1]
      rabbit_ver = File.basename(rabbit_archive,".tar.gz").split('-')[-1]
      rabbit_dir = "rabbitmq_server-#{rabbit_ver}"
      unless File.exist? rabbit_archive
        system "wget", rabbit_url
      end

      unless File.exist? rabbit_dir
        system "tar", "xzvf", rabbit_archive
      end
    end

    desc "tail log files"
    task :tail_logs do
      system "bash", "-c", "tail -f #{$teporingo_root}/tmp/harabbit/rabbit*/logs/rabbit0*.log"
    end

    $rabbit_config["nodes"].each do |node|
      name = node["name"]
      tname = "start_#{name}"
      desc "Start #{name}"
      task tname.to_sym do 
        ENV['RABBITMQ_NODE_IP_ADDRESS'] = node["ip"] || '127.0.0.1'
        ENV['RABBITMQ_NODE_PORT']       = node["port"].to_s
        ENV['RABBITMQ_NODENAME']        = nodename(node)
        ENV['RABBITMQ_MNESIA_BASE']     = "#{$teporingo_root}/tmp/harabbit/#{node["name"]}/mnesia"
        ENV['RABBITMQ_LOG_BASE']        = "#{$teporingo_root}/tmp/harabbit/#{node["name"]}/logs"

        [ ENV['RABBITMQ_MNESIA_BASE'], ENV['RABBITMQ_LOG_BASE'] ].each do |dir|
          FileUtils.mkdir_p(dir) unless File.exist?(dir)
        end

        system "rabbitmq-server"
      end
    end

    %w|list_vhosts list_connections list_channels list_users|.each do |cmd|
      desc cmd
      task cmd.to_sym do
        $rabbit_config["nodes"].each do |node|
          rabbitmqctl node, cmd
        end
      end
    end

    %w|list_permissions list_exchanges list_consumers|.each do |cmd|
      desc cmd
      task cmd.to_sym, :vhost do |t,args|
        vhost = args[:vhost] || "/"
        $rabbit_config["nodes"].each do |node|
          rabbitmqctl node, cmd, '-p', vhost
        end
      end
    end

    desc "list queues"
    task :list_queues, :vhost do |t,args|
      vhost = args[:vhost] || "/"
      fields = %w[name durable auto_delete arguments pid owner_pid
      exclusive_consumer_pid exclusive_consumer_tag 
      messages_ready messages_unacknowledged messages consumers memory]
      $rabbit_config["nodes"].each do |node|
        name = nodename node
        rabbitmqctl node, "list_queues", '-p', vhost, *fields
      end
    end


    desc "list bidnings"
    task :list_bindings, :vhost do |t,args|
      vhost = args[:vhost] || "/"
      fields = %w[source_name source_kind destination_name destination_kind routing_key arguments]
      $rabbit_config["nodes"].each do |node|
        rabbitmqctl node, "list_bindings", '-p', vhost, *fields
      end
    end
  end
end
