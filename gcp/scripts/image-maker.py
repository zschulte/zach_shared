#!/usr/bin/env python

"""
Creates a backup image.tar.gz of an instance.

This script will stop the specified instance.
Please perform a graceful shutdown first.
You will need to restart the instance when finished.
This can be done after the snapshot step is done.

Uses default authentication.  
Either run from an instance with appropriate access
or follow this doc to create and setup a service account:
https://cloud.google.com/docs/authentication/getting-started
"""
import argparse
import datetime
import googleapiclient.discovery
import random
import string
import time

def get_zone(compute, project, instance_name):
    fill = 'name eq ' + instance_name
    dic = compute.instances().aggregatedList(project=project, filter=fill).execute()
    for k, v in dic['items'].items():
        if 'instances' in v.keys():
            zone =  k[6:]
            return zone

def wait_for_it(compute, project, zone, operation):
    timer = 1
    while True:
        working = compute.zoneOperations().get(project=project, zone=zone, operation=operation).execute()
        if working['status'] == 'DONE':
            if 'error' in working:
                raise Exception(working['error'])
            print('Done')
            print
            return working
        time.sleep(min(64, (2 ** timer)) + (random.randint(0, 1000) / 1000))
        timer += 1

def stop_instance(compute, project, zone, instance_name):
    print('Stopping instance ' + instance_name)
    operation = compute.instances().stop(project=project, zone=zone, instance=instance_name).execute()
    wait_for_it(compute, project, zone, operation['name'])

def create_snapshot(compute, project, zone, instance_name, tmpname):
    print('Creating snapshot of disk')
    snapshot_body = {
        'name': tmpname
    }
    operation = compute.disks().createSnapshot(project=project, zone=zone, disk=instance_name, body=snapshot_body).execute()
    wait_for_it(compute, project, zone, operation['name'])

def create_worker_disks(compute, project, zone, instance_name, tmpname):
    sizer = compute.disks().list(project=project, zone=zone).execute()
    for i in range(len(sizer['items'])):
        if sizer['items'][i]['name'] == instance_name:
            disk_size = sizer['items'][i]['sizeGb']
    bigger = int(disk_size) + int(disk_size)
    print('Creating disk from snapshot')
    disk_body = {
        'name': 'image-disk-'+tmpname,
        'sourceSnapshot': 'projects/'+project+'/global/snapshots/'+tmpname
    }
    operation = compute.disks().insert(project=project, zone=zone, body=disk_body).execute()
    wait_for_it(compute, project, zone, operation['name'])
    print('Creating disk to do work on')
    tmpdisk_body = {
        'name': 'tmp-disk-'+tmpname,
        'sizeGb': bigger
    }
    operation = compute.disks().insert(project=project, zone=zone, body=tmpdisk_body).execute()
    wait_for_it(compute, project, zone, operation['name'])
    compute.snapshots().delete(project=project, snapshot=tmpname).execute()

def wait_for_image(storage, bucket, tar_file):
    print('Waiting for image to finish')
    print
    timer = 1
    while True:
        working = storage.objects().list(bucket=bucket).execute()
        if 'error' in working:
            raise Exception(working['error'])
        if len(working['items']) > 1:
            for i in range(len(working['items'])):
                if working['items'][i]['name'] == tar_file + '.tar.gz':
                    print(working['items'][i]['name'] + ' has been created')
                    return working
        time.sleep(min(64, (2 ** timer)) + (random.randint(0, 1000) / 1000))
        timer += 1

def creation(compute, project, zone, tar_file, tmpname, bucket):
    working_image = compute.images().getFromFamily(project='centos-cloud', family='centos-7').execute()['selfLink']
    gross = """#!/bin/bash
        mkdir -p /mnt/tmp
        mkfs.ext4 -F /dev/disk/by-id/google-tmp-disk-%(tmpname)s
        mount -o discard,defaults /dev/disk/by-id/google-tmp-disk-%(tmpname)s /mnt/tmp
        dd if=/dev/disk/by-id/google-image-disk-%(tmpname)s of=/mnt/tmp/disk.raw bs=4096
        tar czf /mnt/tmp/%(tar_file)s.tar.gz /mnt/tmp/disk.raw
        gsutil cp /mnt/tmp/%(tar_file)s.tar.gz gs://%(bucket)s
        gcloud compute instances delete %(tmpname)s --zone=%(zone)s
    """ % {'tar_file': tar_file, 'bucket': bucket, 'zone': zone, 'tmpname': tmpname}
    body = {
        'name': tmpname,
        'machineType': 'zones/' + zone + '/machineTypes/n1-standard-1',
        'serviceAccounts': [{
            'email': 'default',
            'scopes': [
                'https://www.googleapis.com/auth/devstorage.read_write',
                'https://www.googleapis.com/auth/logging.write',
                'https://www.googleapis.com/auth/compute'
            ]
        }],
        'disks': [{
            'boot': True,
            'autoDelete': True,
            'initializeParams': {
                'sourceImage': working_image
            },
        }, {
            'deviceName': 'image-disk-'+tmpname,
            'autoDelete': True,
            'source': 'projects/'+project+'/zones/'+zone+'/disks/image-disk-'+tmpname
        }, {
            'deviceName': 'tmp-disk-'+tmpname,
            'autoDelete': True,
            'source': 'projects/'+project+'/zones/'+zone+'/disks/tmp-disk-'+tmpname
        }],
        'metadata': {
            'items': [{
                'key': 'startup-script',
                'value': gross
            }]
        },
        'networkInterfaces': [{
            'network': 'global/networks/default',
            'accessConfigs': [{
                'type': 'ONE_TO_ONE_NAT',
                'name': 'External NAT'
            }]
        }]
    }
    print('Creating temporary instance to tar up image')
    operation = compute.instances().insert(project=project, zone=zone, body=body).execute()
    wait_for_it(compute, project, zone, operation['name'])

def main(project, instance_name, bucket, tar_file, wait=True):

    compute = googleapiclient.discovery.build('compute', 'v1')
    storage = googleapiclient.discovery.build('storage', 'v1')
    zone = get_zone(compute, project, instance_name)
    tmpname = 'image-maker-'+''.join(random.choice(string.ascii_lowercase) for i in range(10))

    if not tar_file:
        tar_file = instance_name+'-'+datetime.date.today().strftime("%F")+'.image'

    print
    print('#'*31)
    print('#' * 9 + ' IMAGE MAKER ' + '#' * 9)
    print('#'*31)
    print
    print('Instance = '+str(instance_name)) 
    print('Project  = '+str(project))
    print('Zone     = '+str(zone))
    print('Bucket   = '+str(bucket))
    print('Tar File = '+str(tar_file)+'.tar.gz')
    print

### Double check the instance is stopped
    stop_instance(compute, project, zone, instance_name)

### Create snapshot
    create_snapshot(compute, project, zone, instance_name, tmpname)
    
### Create disks
    create_worker_disks(compute, project, zone, instance_name, tmpname)

### Create vm to do the actual work
    creation(compute, project, zone, tar_file, tmpname, bucket)

### Wait until completion
    wait_for_image(storage, bucket, tar_file)

##########################################

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('project',
        help='Project the instance lives in.')
    parser.add_argument('instance',
        help='Instance to be cloned.')
    parser.add_argument('bucket',
        help='Bucket to store the image in.')
    parser.add_argument(
        '-o', '--output', dest="tar_file",
        help='Name of the .tar.gz file to create. Default is INSTANCE-DATE.image.tar.gz')

    args = parser.parse_args()

    main(args.project, args.instance, args.bucket, args.tar_file)

