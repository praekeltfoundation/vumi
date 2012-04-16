Vagrant::Config.run do |config|

  config.vm.box = "lucid32"
  config.vm.box_url = "http://files.vagrantup.com/lucid32.box"

  config.vm.provision :puppet do |puppet|
    puppet.options = "--verbose"
    puppet.manifests_path = "puppet/manifests"
    puppet.manifest_file = "vumi.pp"
  end

end
