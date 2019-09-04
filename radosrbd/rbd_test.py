import myrados

if __name__ == "__main__":
    mycluser=myrados.myrados("/etc/ceph/ceph.conf")
    mycluser.connect()
    mycluser.listpool()