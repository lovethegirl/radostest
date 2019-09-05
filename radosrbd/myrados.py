import rados 
import rbd
class myrados:
    def __init__(self,ceph_conf_path):
        self.cluster=rados.Rados(conffile=ceph_conf_path)
    def connect(self):
        try:
            self.cluster.connect()
        except Exception as e:
            print("connect error:",e)
        print("connect to the cluster")
    def listpool(self):
        pools=self.cluster.list_pools()
        print("pools {pool}".format(pool=pools))
    def createpool(self,pool_name):
        if self.cluster.pool-exists(pool_name):
            return 
        else:
            self.create_pool(pool_name)
    def deletepool(self,pool_name):
        pass
    def createimage(self,)

if __name__ == "__main__":
    mycluster=myrados("/etc/ceph/ceph.conf")
    mycluster.connect()
    mycluster.listpool()
    
