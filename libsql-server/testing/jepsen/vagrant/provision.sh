# install dependencies
apt install -y lxc debootstrap bridge-utils libvirt-clients libvirt-daemon-system iptables ebtables dnsmasq-base libxml2-utils iproute2 leiningen  git

# create lxc containers
for i in {1..10};
    do sudo lxc-create -n n$i -t debian -- --release buster
done

# setup containers network
for i in {1..10}; do
    echo lxc.net.0.type = veth >> /var/lib/lxc/n${i}/config
    echo lxc.net.0.flags = up >> /var/lib/lxc/n${i}/config
    echo lxc.net.0.link = virbr0 >> /var/lib/lxc/n${i}/config
    echo lxc.net.0.hwaddr = 00:1E:62:AA:AA:$(printf "%02x" $i) >> /var/lib/lxc/n${i}/config
done

# setup network bindings
for i in {1..10}; do
  virsh net-update --current default add-last ip-dhcp-host "<host mac=\"00:1E:62:AA:AA:$(printf "%02x" $i)\" name=\"n${i}\" ip=\"192.168.122.1$(printf "%02d" $i)\"/>"
done

# start network
virsh net-autostart default;
virsh net-start default

# setup dhcp
echo "prepend domain-name-servers 192.168.122.1;" >>/etc/dhcp/dhclient.conf

printf "adding key to containers"
cat /home/vagrant/.ssh/id_rsa.pub
for i in {1..10}; do
    mkdir -p /var/lib/lxc/n${i}/rootfs/root/.ssh
    chmod 700 /var/lib/lxc/n${i}/rootfs/root/.ssh/
    cp /home/vagrant/.ssh/id_rsa.pub /var/lib/lxc/n${i}/rootfs/root/.ssh/authorized_keys
    chmod 644 /var/lib/lxc/n${i}/rootfs/root/.ssh/authorized_keys
done

for i in {1..10}; do
    lxc-start -d -n n$i
done

sleep 5

for i in {1..10}; do
    lxc-attach -n n${i} -- bash -c 'echo -e "root\nroot\n" | passwd root';
    lxc-attach -n n${i} -- sed -i 's,^#\?PermitRootLogin .*,PermitRootLogin yes,g' /etc/ssh/sshd_config;
    lxc-attach -n n${i} -- systemctl restart sshd;
done

for n in {1..10}; do ssh-keyscan -t rsa n$n; done >> /home/vagrant/.ssh/known_hosts

for i in {1..10}; do
    lxc-attach -n n${i} -- apt install -y sudo
done
