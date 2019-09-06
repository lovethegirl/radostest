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
#delete snapshot
def purgesnap(rbd_img,snp):
    print("remove {sap}".format(snp=snp))
    rbd_img.remove_snap(snp)
def closeimg(rbd_img):
    rbd_img.close()
# rbd_inst handle
def delimg(ioctx,rbd_inst,imagename):
    print("remove {0} image".format(imagename))
    rbd_inst.remove(ioctx,imagename)    
# clone image from snapshot
# ioctx pool context
# rbd_inst cluster handle
# rbd_img image handle
# snap snapshot name
# cloing cloneimage name
def cloneimage(ioctx,rbd_inst,rbd_img,imagename,snap,cloimg):
    print("protect snapshot")
    rbd_img.protect_snap(snap)
    status = rbd_img.is_protected_snap(snap)
    print("{0} protected status is {1}".format(snap,status))

    print("start clone image")
    rbd_inst.clone(ioctx,imagename,snap,ioctx,cloimg)
    img_list = rbd_inst.list(ioctx)
    clone_img = rbd.Image(ioctx,name=cloimg)
    print("clone_img {0}".format(img_list))
    clone_img.flatten()
    rbd_img.unprotect_snap(snap)
    status = rbd_img.is_protected_snap(snap)
    print("{0} protected status is {1}".format(snap,status))
    return clone_img,img_list

def __main(conf_conf_path,poolname,imagename,snap,clonename):
    cluster = createhandle(ceph_conf_path)#cluster handle
    try:
        connent_ceph(cluster)
        ioctx=create_pool(cluster,poolname)#pool context
        try:
            listpool(cluster)
            size= 1*1024*4
            rbd_inst=createimage(size,ioctx,imagename)#create image
            try:
                rbd_img=createsnapshot(ioctx,imagename,snap)
                try:
                    clone_img, mg_list=cloneimage(ioctx,rbd_inst,rbd_img,imagename,snap,clonename)
                    closeimg(clone_img)
                finally:
                    delimg(ioctx,rbd_inst,clonename)
            finally:
                purgesnap(rbd_img,snap)
        finally:
            closeimg(rbd_img)
            delimg(ioctx,rbd_inst,imagename)
    finally:
        ioctx.close()
        print("ioctx close")
    cluster.shutdown()
    print("cluster close")
if __name__ == "__main__":
    ceph_conf_path = "/etc/ceph/ceph.conf"  
    poolname="rbd"
    imagename="ljw-test"
    snap="ljw-test@snapname"
    clonename="ljw-test-clone"
    cluster = createhandle(ceph_conf_path)#cluster handle
    try:
        connent_ceph(cluster)
        ioctx=create_pool(cluster,poolname)#pool context
        try:
            listpool(cluster)
            size= 1*1024*4
            rbd_inst=createimage(size,ioctx,imagename)#create image
            try:
                rbd_img=createsnapshot(ioctx,imagename,snap)
                try:
                    clone_img, mg_list=cloneimage(ioctx,rbd_inst,rbd_img,imagename,snap,clonename)
                    closeimg(clone_img)
                finally:
                    delimg(ioctx,rbd_inst,clonename)
            finally:
                purgesnap(rbd_img,snap)
        finally:
            closeimg(rbd_img)
            delimg(ioctx,rbd_inst,imagename)
    finally:
        ioctx.close()
        print("ioctx close")
    cluster.shutdown()
    print("cluster close")









