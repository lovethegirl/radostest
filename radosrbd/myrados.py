import rados 
import rbd
class myrados:
    def __init__(self,ceph_conf_path):
        self.cluster=rados.Rados(conffile=ceph_conf_path)
    def connect():
        try:
            self.cluster.connect()
        except Exception as e:
            print("connect error:",e)
        print("connect to the cluster")
    def listpool():
        pools=self.cluster.list_pools()
        print("pools {pool}".format(pool=pool1))
if __name__ == "__main__":
    mycluster=myrados("/etc/ceph/ceph.conf")
    mycluster.connect()
    mycluster.listpool()
    