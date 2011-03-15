Vagrant::Config.run do |config|
  
  config.vm.box = "lucid32"
  config.vm.box_url = "http://files.vagrantup.com/lucid32.box"
  
  config.vm.provision :puppet, :options => "--verbose --debug" do |puppet|
    puppet.manifests_path = "manifests"
    puppet.manifest_file = "vumi.pp"
  end
  
  config.vm.forward_port "web", 9000, 9000
  config.vm.forward_port "smsc", 9001, 9001
  config.vm.forward_port "supervisord", 9010, 9010
end
