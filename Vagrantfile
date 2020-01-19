# -*- mode: ruby -*-
# vi: set ft=ruby :

boxes = [
{
:name => "hadoop-master",
:eth1 => "192.168.5.10",
:mem => "2000",
:cpu => "1",
#:box => "ubuntu/xenial64",
:box => "ubuntu/trusty64",
:files => [
{
    :src => "files/system/scripts",
    :dst => "/home/vagrant/scripts"}
],
:shells => [
    "shells/system/copydebs.sh",
    "shells/system/generic.sh",
    "shells/system/gethadoop.sh",
    "shells/system/ssh.sh",
    "shells/system/monitoring.sh"
],
:ports_forward => [
{
:guest => "50070", :host => "50070"},
{
:guest => "19888", :host => "19888"},
{
:guest => "8088", :host => "8088"},
{
:guest => "9000", :host => "9000"},
{
:name => "grafana",
:guest => "3000", :host => "3000"}
]
},
{
:name => "hadoop-slave1",
:eth1 => "192.168.5.11",
:mem => "2000",
:cpu => "1",
:box => "ubuntu/trusty64",
:files => [
{
    :src => "files/system/scripts",
    :dst => "/home/vagrant/scripts"}
],
:shells => [
    "shells/system/copydebs.sh",
    "shells/system/generic.sh",
    "shells/system/ssh.sh",
    "shells/system/gethadoopslaves.sh",
    "shells/system/monitoring.sh"
],
:ports_forward => [
{
:name => "grafana",
:guest => "3000", :host => "3001"}
]
},
{
:name => "hadoop-slave2",
:eth1 => "192.168.5.12",
:mem => "2000",
:cpu => "1",
:box => "ubuntu/trusty64",
:files => [
{
    :src => "files/system/scripts",
    :dst => "/home/vagrant/scripts"}
],
:shells => [
    "shells/system/copydebs.sh",
    "shells/system/generic.sh",
    "shells/system/ssh.sh",
    "shells/system/gethadoopslaves.sh",
    "shells/system/monitoring.sh"
],
:ports_forward => [
{
:name => "grafana",
:guest => "3000", :host => "3002"}
]
}
]

Vagrant.configure(2) do |config|
    boxes.each do |opts|

        config.vm.define opts[:name] do |config|
            config.vbguest.auto_update = false
            config.vm.provider "virtualbox" do |v, override|
                override.vm.box = opts[:box]
            end
            config.vm.hostname = opts[:name]
            config.vm.provider "virtualbox" do |v|
                v.customize ["modifyvm", :id, "--memory", opts[:mem]]
                v.customize ["modifyvm", :id, "--cpus", opts[:cpu]]
            end

            config.vm.network :private_network, ip: opts[:eth1]

            opts[:ports_forward].each do |item|
                config.vm.network "forwarded_port", guest: item[:guest], host: item[:host]
            end

            opts[:files].each do |fileitem|
                config.vm.provision :file do |file|
                    file.source = fileitem[:src]
                    file.destination = fileitem[:dst]
                end
            end
            opts[:shells].each do |item|
                config.vm.provision :shell, path: item
            end
        end
    end
end
