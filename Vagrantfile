Vagrant.configure("2") do |config|
  config.vm.provider "virtualbox" do |v|
    host = RbConfig::CONFIG['host_os']

    # Give VM 1/4 system memory & access to all cpu cores on the host
    if host =~ /darwin/
      cpus = `sysctl -n hw.ncpu`.to_i
      # sysctl returns Bytes and we need to convert to MB
      mem = `sysctl -n hw.memsize`.to_i / 1024 / 1024 / 4
    elsif host =~ /linux/
      cpus = `nproc`.to_i
      # meminfo shows KB and we need to convert to MB
      mem = `grep 'MemTotal' /proc/meminfo | sed -e 's/MemTotal://' -e 's/ kB//'`.to_i / 1024 / 4
    else # sorry Windows folks, I can't help you
      cpus = 2
      mem = 1024
    end

    v.customize ["modifyvm", :id, "--memory", mem]
    v.customize ["modifyvm", :id, "--cpus", cpus]
  end

  config.vm.define "dev" do |dev|
    dev.ssh.insert_key = false
    dev.vm.box = "ubuntu/trusty64"
    dev.vm.network :forwarded_port, host: 5000, guest: 5000
    dev.vm.network :forwarded_port, host: 5432, guest: 5432
    dev.vm.provision "ansible" do |ansible|
      ansible.playbook = "ansible/dev.yml"
    end
  end
end
