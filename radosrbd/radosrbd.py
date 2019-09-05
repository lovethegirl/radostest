import rados
import rbd
def createhandle(ceph_conf_path):
    try:
        cluster = rados.Rados(conffile=ceph_conf_path)
    except TypeError as e:
        print ('Argument validation error: ', e)
        raise e
    print("create cluster handle \n")
    return cluster
def connent_ceph(cluster):
    try:
        cluster.connect()
    except Exception as e:
        print("connection error: ", e)
        raise e
    finally:
        print("Connected to the cluster.")
def create_pool(cluster,pool_name):
    if cluster.pool_exists(pool_name):
        pass
    else:
        cluster.create_pool(pool_name)
    ioctx = cluster.open_ioctx(pool_name)
    return ioctx
def listpool(cluster):
    pools=cluster.list_pools()
    print("pools {pool}".format(pool=pools))
def deletepool(self,pool_name):
    pass
def createimage(size,ioctx,imagename):
    rbd_inst = rbd.RBD()
    rbd_inst.create(ioctx,imagename,size)
    img_list = rbd_inst.list(ioctx)
    print("after create images {0}".format(img_list))
    return rbd_inst
def createsnapshot(ioctx,imagename,snap):
    rbd_img=rbd.Image(ioctx,name=imagename)
    rbd_img.create_snap(snap)
    print("snap list")
    snap_list=rbd_img.list_snaps()
    for item in snap_list:
        print(item['name'])
    return rbd_img
def purgesnap(rbd_img,snp):
    print("remove snapshot")
    rbd_img.remove_snap(snp)
def closeimg(rbd_img):
    rbd_img.close()
def delimg(ioctx,rbd_inst,imagename):
    print("remove rbd image")
    rbd_inst.remove(ioctx,imagename)    
# def delrbd(rbd,ioctx):
#     rbd.remove(ioctx,IMG)
#     rbd.remove(ioctx,CLN)
if __name__ == "__main__":
    ceph_conf_path = "/etc/ceph/ceph.conf"    
    cluster = createhandle(ceph_conf_path)#cluster handle
    try:
        connent_ceph(cluster)
        ioctx=create_pool(cluster,"rbd")#pool context
        try:
            listpool(cluster)
            size= 1*1024*4
            rbd_inst=createimage(size,ioctx,"ljw-test")#create image
            try:
                rbd_img=createsnapshot(ioctx,"ljw-test","ljw-test@snapname")
            finally:
                purgesnap(rbd_img,"ljw-test@snapname")
        finally:
            closeimg(rbd_img)
            delimg(ioctx,rbd_inst,"ljw-test")
    finally:
        ioctx.close()
        print("ioctx close")
    cluster.shutdown()
    print("cluster close")









