#!/usr/bin/env python
# -*- coding: utf-8 -*-

#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from __future__ import with_statement

import logging
import os
import pipes
import random
import shutil
import string
import subprocess
import sys
import tempfile
import time
import urllib2
import warnings
from optparse import OptionParser
from sys import stderr
import boto
from boto.ec2.blockdevicemapping import BlockDeviceMapping, BlockDeviceType, EBSBlockDeviceType
from boto import ec2

DEFAULT_SPARK_VERSION = "1.2.1"
SPARK_EC2_DIR = os.path.dirname(os.path.realpath(__file__))

MESOS_SPARK_EC2_BRANCH = "v4"
# A URL prefix from which to fetch AMI information
AMI_PREFIX = "https://raw.github.com/mesos/spark-ec2/{b}/ami-list".format(b=MESOS_SPARK_EC2_BRANCH)


class UsageError(Exception):
    pass


# Configure and parse our command-line arguments
def parse_args():
    parser = OptionParser(
        usage="spark-ec2 [options] <action> <cluster_name>"
        + "\n\n<action> can be: launch, destroy, login, stop, start, get-main, reboot-subordinates",
        add_help_option=False)
    parser.add_option(
        "-h", "--help", action="help",
        help="Show this help message and exit")
    parser.add_option(
        "-s", "--subordinates", type="int", default=1,
        help="Number of subordinates to launch (default: %default)")
    parser.add_option(
        "-w", "--wait", type="int",
        help="DEPRECATED (no longer necessary) - Seconds to wait for nodes to start")
    parser.add_option(
        "-k", "--key-pair",
        help="Key pair to use on instances")
    parser.add_option(
        "-i", "--identity-file",
        help="SSH private key file to use for logging into instances")
    parser.add_option(
        "-t", "--instance-type", default="m1.large",
        help="Type of instance to launch (default: %default). " +
             "WARNING: must be 64-bit; small instances won't work")
    parser.add_option(
        "-m", "--main-instance-type", default="",
        help="Main instance type (leave empty for same as instance-type)")
    parser.add_option(
        "-r", "--region", default="us-east-1",
        help="EC2 region zone to launch instances in")
    parser.add_option(
        "-z", "--zone", default="",
        help="Availability zone to launch instances in, or 'all' to spread " +
             "subordinates across multiple (an additional $0.01/Gb for bandwidth" +
             "between zones applies)")
    parser.add_option("-a", "--ami", help="Amazon Machine Image ID to use")
    parser.add_option(
        "-v", "--spark-version", default=DEFAULT_SPARK_VERSION,
        help="Version of Spark to use: 'X.Y.Z' or a specific git hash (default: %default)")
    parser.add_option(
        "--spark-git-repo",
        default="https://github.com/apache/spark",
        help="Github repo from which to checkout supplied commit hash")
    parser.add_option(
        "--hadoop-major-version", default="1",
        help="Major version of Hadoop (default: %default)")
    parser.add_option(
        "-D", metavar="[ADDRESS:]PORT", dest="proxy_port",
        help="Use SSH dynamic port forwarding to create a SOCKS proxy at " +
             "the given local address (for use with login)")
    parser.add_option(
        "--resume", action="store_true", default=False,
        help="Resume installation on a previously launched cluster " +
             "(for debugging)")
    parser.add_option(
        "--ebs-vol-size", metavar="SIZE", type="int", default=0,
        help="Size (in GB) of each EBS volume.")
    parser.add_option(
        "--ebs-vol-type", default="standard",
        help="EBS volume type (e.g. 'gp2', 'standard').")
    parser.add_option(
        "--ebs-vol-num", type="int", default=1,
        help="Number of EBS volumes to attach to each node as /vol[x]. " +
             "The volumes will be deleted when the instances terminate. " +
             "Only possible on EBS-backed AMIs. " +
             "EBS volumes are only attached if --ebs-vol-size > 0." +
             "Only support up to 8 EBS volumes.")
    parser.add_option(
        "--swap", metavar="SWAP", type="int", default=1024,
        help="Swap space to set up per node, in MB (default: %default)")
    parser.add_option(
        "--spot-price", metavar="PRICE", type="float",
        help="If specified, launch subordinates as spot instances with the given " +
             "maximum price (in dollars)")
    parser.add_option(
        "--ganglia", action="store_true", default=True,
        help="Setup Ganglia monitoring on cluster (default: %default). NOTE: " +
             "the Ganglia page will be publicly accessible")
    parser.add_option(
        "--no-ganglia", action="store_false", dest="ganglia",
        help="Disable Ganglia monitoring for the cluster")
    parser.add_option(
        "-u", "--user", default="root",
        help="The SSH user you want to connect as (default: %default)")
    parser.add_option(
        "--delete-groups", action="store_true", default=False,
        help="When destroying a cluster, delete the security groups that were created")
    parser.add_option(
        "--use-existing-main", action="store_true", default=False,
        help="Launch fresh subordinates, but use an existing stopped main if possible")
    parser.add_option(
        "--worker-instances", type="int", default=1,
        help="Number of instances per worker: variable SPARK_WORKER_INSTANCES (default: %default)")
    parser.add_option(
        "--main-opts", type="string", default="",
        help="Extra options to give to main through SPARK_MASTER_OPTS variable " +
             "(e.g -Dspark.worker.timeout=180)")
    parser.add_option(
        "--user-data", type="string", default="",
        help="Path to a user-data file (most AMI's interpret this as an initialization script)")
    parser.add_option(
        "--authorized-address", type="string", default="0.0.0.0/0",
        help="Address to authorize on created security groups (default: %default)")
    parser.add_option(
        "--additional-security-group", type="string", default="",
        help="Additional security group to place the machines in")
    parser.add_option(
        "--copy-aws-credentials", action="store_true", default=False,
        help="Add AWS credentials to hadoop configuration to allow Spark to access S3")

    (opts, args) = parser.parse_args()
    if len(args) != 2:
        parser.print_help()
        sys.exit(1)
    (action, cluster_name) = args

    # Boto config check
    # http://boto.cloudhackers.com/en/latest/boto_config_tut.html
    home_dir = os.getenv('HOME')
    if home_dir is None or not os.path.isfile(home_dir + '/.boto'):
        if not os.path.isfile('/etc/boto.cfg'):
            if os.getenv('AWS_ACCESS_KEY_ID') is None:
                print >> stderr, ("ERROR: The environment variable AWS_ACCESS_KEY_ID " +
                                  "must be set")
                sys.exit(1)
            if os.getenv('AWS_SECRET_ACCESS_KEY') is None:
                print >> stderr, ("ERROR: The environment variable AWS_SECRET_ACCESS_KEY " +
                                  "must be set")
                sys.exit(1)
    return (opts, action, cluster_name)


# Get the EC2 security group of the given name, creating it if it doesn't exist
def get_or_make_group(conn, name):
    groups = conn.get_all_security_groups()
    group = [g for g in groups if g.name == name]
    if len(group) > 0:
        return group[0]
    else:
        print "Creating security group " + name
        return conn.create_security_group(name, "Spark EC2 group")


# Check whether a given EC2 instance object is in a state we consider active,
# i.e. not terminating or terminated. We count both stopping and stopped as
# active since we can restart stopped clusters.
def is_active(instance):
    return (instance.state in ['pending', 'running', 'stopping', 'stopped'])


# Return correct versions of Spark and Shark, given the supplied Spark version
def get_spark_shark_version(opts):
    spark_shark_map = {
        "0.7.3": "0.7.1",
        "0.8.0": "0.8.0",
        "0.8.1": "0.8.1",
        "0.9.0": "0.9.0",
        "0.9.1": "0.9.1",
        # These are dummy versions (no Shark versions after this)
        "1.0.0": "1.0.0",
        "1.0.1": "1.0.1",
        "1.0.2": "1.0.2",
        "1.1.0": "1.1.0",
        "1.1.1": "1.1.1",
        "1.2.0": "1.2.0",
        "1.2.1": "1.2.1",
    }
    version = opts.spark_version.replace("v", "")
    if version not in spark_shark_map:
        print >> stderr, "Don't know about Spark version: %s" % version
        sys.exit(1)
    return (version, spark_shark_map[version])


# Attempt to resolve an appropriate AMI given the architecture and region of the request.
# Source: http://aws.amazon.com/amazon-linux-ami/instance-type-matrix/
# Last Updated: 2014-06-20
# For easy maintainability, please keep this manually-inputted dictionary sorted by key.
def get_spark_ami(opts):
    instance_types = {
        "c1.medium":   "pvm",
        "c1.xlarge":   "pvm",
        "c3.2xlarge":  "pvm",
        "c3.4xlarge":  "pvm",
        "c3.8xlarge":  "pvm",
        "c3.large":    "pvm",
        "c3.xlarge":   "pvm",
        "cc1.4xlarge": "hvm",
        "cc2.8xlarge": "hvm",
        "cg1.4xlarge": "hvm",
        "cr1.8xlarge": "hvm",
        "hi1.4xlarge": "pvm",
        "hs1.8xlarge": "pvm",
        "i2.2xlarge":  "hvm",
        "i2.4xlarge":  "hvm",
        "i2.8xlarge":  "hvm",
        "i2.xlarge":   "hvm",
        "m1.large":    "pvm",
        "m1.medium":   "pvm",
        "m1.small":    "pvm",
        "m1.xlarge":   "pvm",
        "m2.2xlarge":  "pvm",
        "m2.4xlarge":  "pvm",
        "m2.xlarge":   "pvm",
        "m3.2xlarge":  "hvm",
        "m3.large":    "hvm",
        "m3.medium":   "hvm",
        "m3.xlarge":   "hvm",
        "r3.2xlarge":  "hvm",
        "r3.4xlarge":  "hvm",
        "r3.8xlarge":  "hvm",
        "r3.large":    "hvm",
        "r3.xlarge":   "hvm",
        "t1.micro":    "pvm",
        "t2.medium":   "hvm",
        "t2.micro":    "hvm",
        "t2.small":    "hvm",
    }
    if opts.instance_type in instance_types:
        instance_type = instance_types[opts.instance_type]
    else:
        instance_type = "pvm"
        print >> stderr,\
            "Don't recognize %s, assuming type is pvm" % opts.instance_type

    ami_path = "%s/%s/%s" % (AMI_PREFIX, opts.region, instance_type)
    try:
        ami = urllib2.urlopen(ami_path).read().strip()
        print "Spark AMI: " + ami
    except:
        print >> stderr, "Could not resolve AMI at: " + ami_path
        sys.exit(1)

    return ami


# Launch a cluster of the given name, by setting up its security groups,
# and then starting new instances in them.
# Returns a tuple of EC2 reservation objects for the main and subordinates
# Fails if there already instances running in the cluster's groups.
def launch_cluster(conn, opts, cluster_name):
    if opts.identity_file is None:
        print >> stderr, "ERROR: Must provide an identity file (-i) for ssh connections."
        sys.exit(1)
    if opts.key_pair is None:
        print >> stderr, "ERROR: Must provide a key pair name (-k) to use on instances."
        sys.exit(1)

    user_data_content = None
    if opts.user_data:
        with open(opts.user_data) as user_data_file:
            user_data_content = user_data_file.read()

    print "Setting up security groups..."
    main_group = get_or_make_group(conn, cluster_name + "-main")
    subordinate_group = get_or_make_group(conn, cluster_name + "-subordinates")
    authorized_address = opts.authorized_address
    if main_group.rules == []:  # Group was just now created
        main_group.authorize(src_group=main_group)
        main_group.authorize(src_group=subordinate_group)
        main_group.authorize('tcp', 22, 22, authorized_address)
        main_group.authorize('tcp', 8080, 8081, authorized_address)
        main_group.authorize('tcp', 18080, 18080, authorized_address)
        main_group.authorize('tcp', 19999, 19999, authorized_address)
        main_group.authorize('tcp', 50030, 50030, authorized_address)
        main_group.authorize('tcp', 50070, 50070, authorized_address)
        main_group.authorize('tcp', 60070, 60070, authorized_address)
        main_group.authorize('tcp', 4040, 4045, authorized_address)
        if opts.ganglia:
            main_group.authorize('tcp', 5080, 5080, authorized_address)
    if subordinate_group.rules == []:  # Group was just now created
        subordinate_group.authorize(src_group=main_group)
        subordinate_group.authorize(src_group=subordinate_group)
        subordinate_group.authorize('tcp', 22, 22, authorized_address)
        subordinate_group.authorize('tcp', 8080, 8081, authorized_address)
        subordinate_group.authorize('tcp', 50060, 50060, authorized_address)
        subordinate_group.authorize('tcp', 50075, 50075, authorized_address)
        subordinate_group.authorize('tcp', 60060, 60060, authorized_address)
        subordinate_group.authorize('tcp', 60075, 60075, authorized_address)

    # Check if instances are already running in our groups
    existing_mains, existing_subordinates = get_existing_cluster(conn, opts, cluster_name,
                                                             die_on_error=False)
    if existing_subordinates or (existing_mains and not opts.use_existing_main):
        print >> stderr, ("ERROR: There are already instances running in " +
                          "group %s or %s" % (main_group.name, subordinate_group.name))
        sys.exit(1)

    # Figure out Spark AMI
    if opts.ami is None:
        opts.ami = get_spark_ami(opts)

    additional_groups = []
    if opts.additional_security_group:
        additional_groups = [sg
                             for sg in conn.get_all_security_groups()
                             if opts.additional_security_group in (sg.name, sg.id)]
    print "Launching instances..."

    try:
        image = conn.get_all_images(image_ids=[opts.ami])[0]
    except:
        print >> stderr, "Could not find AMI " + opts.ami
        sys.exit(1)

    # Create block device mapping so that we can add EBS volumes if asked to.
    # The first drive is attached as /dev/sds, 2nd as /dev/sdt, ... /dev/sdz
    block_map = BlockDeviceMapping()
    if opts.ebs_vol_size > 0:
        for i in range(opts.ebs_vol_num):
            device = EBSBlockDeviceType()
            device.size = opts.ebs_vol_size
            device.volume_type = opts.ebs_vol_type
            device.delete_on_termination = True
            block_map["/dev/sd" + chr(ord('s') + i)] = device

    # AWS ignores the AMI-specified block device mapping for M3 (see SPARK-3342).
    if opts.instance_type.startswith('m3.'):
        for i in range(get_num_disks(opts.instance_type)):
            dev = BlockDeviceType()
            dev.ephemeral_name = 'ephemeral%d' % i
            # The first ephemeral drive is /dev/sdb.
            name = '/dev/sd' + string.letters[i + 1]
            block_map[name] = dev

    # Launch subordinates
    if opts.spot_price is not None:
        # Launch spot instances with the requested price
        print ("Requesting %d subordinates as spot instances with price $%.3f" %
               (opts.subordinates, opts.spot_price))
        zones = get_zones(conn, opts)
        num_zones = len(zones)
        i = 0
        my_req_ids = []
        for zone in zones:
            num_subordinates_this_zone = get_partition(opts.subordinates, num_zones, i)
            subordinate_reqs = conn.request_spot_instances(
                price=opts.spot_price,
                image_id=opts.ami,
                launch_group="launch-group-%s" % cluster_name,
                placement=zone,
                count=num_subordinates_this_zone,
                key_name=opts.key_pair,
                security_groups=[subordinate_group] + additional_groups,
                instance_type=opts.instance_type,
                block_device_map=block_map,
                user_data=user_data_content)
            my_req_ids += [req.id for req in subordinate_reqs]
            i += 1

        print "Waiting for spot instances to be granted..."
        try:
            while True:
                time.sleep(10)
                reqs = conn.get_all_spot_instance_requests()
                id_to_req = {}
                for r in reqs:
                    id_to_req[r.id] = r
                active_instance_ids = []
                for i in my_req_ids:
                    if i in id_to_req and id_to_req[i].state == "active":
                        active_instance_ids.append(id_to_req[i].instance_id)
                if len(active_instance_ids) == opts.subordinates:
                    print "All %d subordinates granted" % opts.subordinates
                    reservations = conn.get_all_instances(active_instance_ids)
                    subordinate_nodes = []
                    for r in reservations:
                        subordinate_nodes += r.instances
                    break
                else:
                    print "%d of %d subordinates granted, waiting longer" % (
                        len(active_instance_ids), opts.subordinates)
        except:
            print "Canceling spot instance requests"
            conn.cancel_spot_instance_requests(my_req_ids)
            # Log a warning if any of these requests actually launched instances:
            (main_nodes, subordinate_nodes) = get_existing_cluster(
                conn, opts, cluster_name, die_on_error=False)
            running = len(main_nodes) + len(subordinate_nodes)
            if running:
                print >> stderr, ("WARNING: %d instances are still running" % running)
            sys.exit(0)
    else:
        # Launch non-spot instances
        zones = get_zones(conn, opts)
        num_zones = len(zones)
        i = 0
        subordinate_nodes = []
        for zone in zones:
            num_subordinates_this_zone = get_partition(opts.subordinates, num_zones, i)
            if num_subordinates_this_zone > 0:
                subordinate_res = image.run(key_name=opts.key_pair,
                                      security_groups=[subordinate_group] + additional_groups,
                                      instance_type=opts.instance_type,
                                      placement=zone,
                                      min_count=num_subordinates_this_zone,
                                      max_count=num_subordinates_this_zone,
                                      block_device_map=block_map,
                                      user_data=user_data_content)
                subordinate_nodes += subordinate_res.instances
                print "Launched %d subordinates in %s, regid = %s" % (num_subordinates_this_zone,
                                                                zone, subordinate_res.id)
            i += 1

    # Launch or resume mains
    if existing_mains:
        print "Starting main..."
        for inst in existing_mains:
            if inst.state not in ["shutting-down", "terminated"]:
                inst.start()
        main_nodes = existing_mains
    else:
        main_type = opts.main_instance_type
        if main_type == "":
            main_type = opts.instance_type
        if opts.zone == 'all':
            opts.zone = random.choice(conn.get_all_zones()).name
        main_res = image.run(key_name=opts.key_pair,
                               security_groups=[main_group] + additional_groups,
                               instance_type=main_type,
                               placement=opts.zone,
                               min_count=1,
                               max_count=1,
                               block_device_map=block_map,
                               user_data=user_data_content)
        main_nodes = main_res.instances
        print "Launched main in %s, regid = %s" % (zone, main_res.id)

    # Give the instances descriptive names
    for main in main_nodes:
        main.add_tag(
            key='Name',
            value='{cn}-main-{iid}'.format(cn=cluster_name, iid=main.id))
    for subordinate in subordinate_nodes:
        subordinate.add_tag(
            key='Name',
            value='{cn}-subordinate-{iid}'.format(cn=cluster_name, iid=subordinate.id))

    # Return all the instances
    return (main_nodes, subordinate_nodes)


# Get the EC2 instances in an existing cluster if available.
# Returns a tuple of lists of EC2 instance objects for the mains and subordinates


def get_existing_cluster(conn, opts, cluster_name, die_on_error=True):
    print "Searching for existing cluster " + cluster_name + "..."
    reservations = conn.get_all_instances()
    main_nodes = []
    subordinate_nodes = []
    for res in reservations:
        active = [i for i in res.instances if is_active(i)]
        for inst in active:
            group_names = [g.name for g in inst.groups]
            if (cluster_name + "-main") in group_names:
                main_nodes.append(inst)
            elif (cluster_name + "-subordinates") in group_names:
                subordinate_nodes.append(inst)
    if any((main_nodes, subordinate_nodes)):
        print "Found %d main(s), %d subordinates" % (len(main_nodes), len(subordinate_nodes))
    if main_nodes != [] or not die_on_error:
        return (main_nodes, subordinate_nodes)
    else:
        if main_nodes == [] and subordinate_nodes != []:
            print >> sys.stderr, "ERROR: Could not find main in group " + cluster_name + "-main"
        else:
            print >> sys.stderr, "ERROR: Could not find any existing cluster"
        sys.exit(1)


# Deploy configuration files and run setup scripts on a newly launched
# or started EC2 cluster.


def setup_cluster(conn, main_nodes, subordinate_nodes, opts, deploy_ssh_key):
    main = main_nodes[0].public_dns_name
    if deploy_ssh_key:
        print "Generating cluster's SSH key on main..."
        key_setup = """
          [ -f ~/.ssh/id_rsa ] ||
            (ssh-keygen -q -t rsa -N '' -f ~/.ssh/id_rsa &&
             cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys)
        """
        ssh(main, opts, key_setup)
        dot_ssh_tar = ssh_read(main, opts, ['tar', 'c', '.ssh'])
        print "Transferring cluster's SSH key to subordinates..."
        for subordinate in subordinate_nodes:
            print subordinate.public_dns_name
            ssh_write(subordinate.public_dns_name, opts, ['tar', 'x'], dot_ssh_tar)

    modules = ['spark', 'shark', 'ephemeral-hdfs', 'persistent-hdfs',
               'mapreduce', 'spark-standalone', 'tachyon']

    if opts.hadoop_major_version == "1":
        modules = filter(lambda x: x != "mapreduce", modules)

    if opts.ganglia:
        modules.append('ganglia')

    # NOTE: We should clone the repository before running deploy_files to
    # prevent ec2-variables.sh from being overwritten
    ssh(
        host=main,
        opts=opts,
        command="rm -rf spark-ec2"
        + " && "
        + "git clone https://github.com/mesos/spark-ec2.git -b {b}".format(b=MESOS_SPARK_EC2_BRANCH)
    )

    print "Deploying files to main..."
    deploy_files(
        conn=conn,
        root_dir=SPARK_EC2_DIR + "/" + "deploy.generic",
        opts=opts,
        main_nodes=main_nodes,
        subordinate_nodes=subordinate_nodes,
        modules=modules
    )

    print "Running setup on main..."
    setup_spark_cluster(main, opts)
    print "Done!"


def setup_standalone_cluster(main, subordinate_nodes, opts):
    subordinate_ips = '\n'.join([i.public_dns_name for i in subordinate_nodes])
    ssh(main, opts, "echo \"%s\" > spark/conf/subordinates" % (subordinate_ips))
    ssh(main, opts, "/root/spark/sbin/start-all.sh")


def setup_spark_cluster(main, opts):
    ssh(main, opts, "chmod u+x spark-ec2/setup.sh")
    ssh(main, opts, "spark-ec2/setup.sh")
    print "Spark standalone cluster started at http://%s:8080" % main

    if opts.ganglia:
        print "Ganglia started at http://%s:5080/ganglia" % main


def is_ssh_available(host, opts):
    "Checks if SSH is available on the host."
    try:
        with open(os.devnull, 'w') as devnull:
            ret = subprocess.check_call(
                ssh_command(opts) + ['-t', '-t', '-o', 'ConnectTimeout=3',
                                     '%s@%s' % (opts.user, host), stringify_command('true')],
                stdout=devnull,
                stderr=devnull
            )
        return ret == 0
    except subprocess.CalledProcessError as e:
        return False


def is_cluster_ssh_available(cluster_instances, opts):
    for i in cluster_instances:
        if not is_ssh_available(host=i.ip_address, opts=opts):
            return False
    else:
        return True


def wait_for_cluster_state(cluster_instances, cluster_state, opts):
    """
    cluster_instances: a list of boto.ec2.instance.Instance
    cluster_state: a string representing the desired state of all the instances in the cluster
           value can be 'ssh-ready' or a valid value from boto.ec2.instance.InstanceState such as
           'running', 'terminated', etc.
           (would be nice to replace this with a proper enum: http://stackoverflow.com/a/1695250)
    """
    sys.stdout.write(
        "Waiting for all instances in cluster to enter '{s}' state.".format(s=cluster_state)
    )
    sys.stdout.flush()

    num_attempts = 0

    while True:
        time.sleep(3 * num_attempts)

        for i in cluster_instances:
            s = i.update()  # capture output to suppress print to screen in newer versions of boto

        if cluster_state == 'ssh-ready':
            if all(i.state == 'running' for i in cluster_instances) and \
               is_cluster_ssh_available(cluster_instances, opts):
                break
        else:
            if all(i.state == cluster_state for i in cluster_instances):
                break

        num_attempts += 1

        sys.stdout.write(".")
        sys.stdout.flush()

    sys.stdout.write("\n")


# Get number of local disks available for a given EC2 instance type.
def get_num_disks(instance_type):
    # Source: http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/InstanceStorage.html
    # Last Updated: 2014-06-20
    # For easy maintainability, please keep this manually-inputted dictionary sorted by key.
    disks_by_instance = {
        "c1.medium":   1,
        "c1.xlarge":   4,
        "c3.2xlarge":  2,
        "c3.4xlarge":  2,
        "c3.8xlarge":  2,
        "c3.large":    2,
        "c3.xlarge":   2,
        "cc1.4xlarge": 2,
        "cc2.8xlarge": 4,
        "cg1.4xlarge": 2,
        "cr1.8xlarge": 2,
        "g2.2xlarge":  1,
        "hi1.4xlarge": 2,
        "hs1.8xlarge": 24,
        "i2.2xlarge":  2,
        "i2.4xlarge":  4,
        "i2.8xlarge":  8,
        "i2.xlarge":   1,
        "m1.large":    2,
        "m1.medium":   1,
        "m1.small":    1,
        "m1.xlarge":   4,
        "m2.2xlarge":  1,
        "m2.4xlarge":  2,
        "m2.xlarge":   1,
        "m3.2xlarge":  2,
        "m3.large":    1,
        "m3.medium":   1,
        "m3.xlarge":   2,
        "r3.2xlarge":  1,
        "r3.4xlarge":  1,
        "r3.8xlarge":  2,
        "r3.large":    1,
        "r3.xlarge":   1,
        "t1.micro":    0,
    }
    if instance_type in disks_by_instance:
        return disks_by_instance[instance_type]
    else:
        print >> stderr, ("WARNING: Don't know number of disks on instance type %s; assuming 1"
                          % instance_type)
        return 1


# Deploy the configuration file templates in a given local directory to
# a cluster, filling in any template parameters with information about the
# cluster (e.g. lists of mains and subordinates). Files are only deployed to
# the first main instance in the cluster, and we expect the setup
# script to be run on that instance to copy them to other nodes.
#
# root_dir should be an absolute path to the directory with the files we want to deploy.
def deploy_files(conn, root_dir, opts, main_nodes, subordinate_nodes, modules):
    active_main = main_nodes[0].public_dns_name

    num_disks = get_num_disks(opts.instance_type)
    hdfs_data_dirs = "/mnt/ephemeral-hdfs/data"
    mapred_local_dirs = "/mnt/hadoop/mrlocal"
    spark_local_dirs = "/mnt/spark"
    if num_disks > 1:
        for i in range(2, num_disks + 1):
            hdfs_data_dirs += ",/mnt%d/ephemeral-hdfs/data" % i
            mapred_local_dirs += ",/mnt%d/hadoop/mrlocal" % i
            spark_local_dirs += ",/mnt%d/spark" % i

    cluster_url = "%s:7077" % active_main

    if "." in opts.spark_version:
        # Pre-built spark & shark deploy
        (spark_v, shark_v) = get_spark_shark_version(opts)
    else:
        # Spark-only custom deploy
        spark_v = "%s|%s" % (opts.spark_git_repo, opts.spark_version)
        shark_v = ""
        modules = filter(lambda x: x != "shark", modules)

    template_vars = {
        "main_list": '\n'.join([i.public_dns_name for i in main_nodes]),
        "active_main": active_main,
        "subordinate_list": '\n'.join([i.public_dns_name for i in subordinate_nodes]),
        "cluster_url": cluster_url,
        "hdfs_data_dirs": hdfs_data_dirs,
        "mapred_local_dirs": mapred_local_dirs,
        "spark_local_dirs": spark_local_dirs,
        "swap": str(opts.swap),
        "modules": '\n'.join(modules),
        "spark_version": spark_v,
        "shark_version": shark_v,
        "hadoop_major_version": opts.hadoop_major_version,
        "spark_worker_instances": "%d" % opts.worker_instances,
        "spark_main_opts": opts.main_opts
    }

    if opts.copy_aws_credentials:
        template_vars["aws_access_key_id"] = conn.aws_access_key_id
        template_vars["aws_secret_access_key"] = conn.aws_secret_access_key
    else:
        template_vars["aws_access_key_id"] = ""
        template_vars["aws_secret_access_key"] = ""

    # Create a temp directory in which we will place all the files to be
    # deployed after we substitue template parameters in them
    tmp_dir = tempfile.mkdtemp()
    for path, dirs, files in os.walk(root_dir):
        if path.find(".svn") == -1:
            dest_dir = os.path.join('/', path[len(root_dir):])
            local_dir = tmp_dir + dest_dir
            if not os.path.exists(local_dir):
                os.makedirs(local_dir)
            for filename in files:
                if filename[0] not in '#.~' and filename[-1] != '~':
                    dest_file = os.path.join(dest_dir, filename)
                    local_file = tmp_dir + dest_file
                    with open(os.path.join(path, filename)) as src:
                        with open(local_file, "w") as dest:
                            text = src.read()
                            for key in template_vars:
                                text = text.replace("{{" + key + "}}", template_vars[key])
                            dest.write(text)
                            dest.close()
    # rsync the whole directory over to the main machine
    command = [
        'rsync', '-rv',
        '-e', stringify_command(ssh_command(opts)),
        "%s/" % tmp_dir,
        "%s@%s:/" % (opts.user, active_main)
    ]
    subprocess.check_call(command)
    # Remove the temp directory we created above
    shutil.rmtree(tmp_dir)


def stringify_command(parts):
    if isinstance(parts, str):
        return parts
    else:
        return ' '.join(map(pipes.quote, parts))


def ssh_args(opts):
    parts = ['-o', 'StrictHostKeyChecking=no']
    if opts.identity_file is not None:
        parts += ['-i', opts.identity_file]
    return parts


def ssh_command(opts):
    return ['ssh'] + ssh_args(opts)


# Run a command on a host through ssh, retrying up to five times
# and then throwing an exception if ssh continues to fail.
def ssh(host, opts, command):
    tries = 0
    while True:
        try:
            return subprocess.check_call(
                ssh_command(opts) + ['-t', '-t', '%s@%s' % (opts.user, host),
                                     stringify_command(command)])
        except subprocess.CalledProcessError as e:
            if tries > 5:
                # If this was an ssh failure, provide the user with hints.
                if e.returncode == 255:
                    raise UsageError(
                        "Failed to SSH to remote host {0}.\n" +
                        "Please check that you have provided the correct --identity-file and " +
                        "--key-pair parameters and try again.".format(host))
                else:
                    raise e
            print >> stderr, \
                "Error executing remote command, retrying after 30 seconds: {0}".format(e)
            time.sleep(30)
            tries = tries + 1


# Backported from Python 2.7 for compatiblity with 2.6 (See SPARK-1990)
def _check_output(*popenargs, **kwargs):
    if 'stdout' in kwargs:
        raise ValueError('stdout argument not allowed, it will be overridden.')
    process = subprocess.Popen(stdout=subprocess.PIPE, *popenargs, **kwargs)
    output, unused_err = process.communicate()
    retcode = process.poll()
    if retcode:
        cmd = kwargs.get("args")
        if cmd is None:
            cmd = popenargs[0]
        raise subprocess.CalledProcessError(retcode, cmd, output=output)
    return output


def ssh_read(host, opts, command):
    return _check_output(
        ssh_command(opts) + ['%s@%s' % (opts.user, host), stringify_command(command)])


def ssh_write(host, opts, command, arguments):
    tries = 0
    while True:
        proc = subprocess.Popen(
            ssh_command(opts) + ['%s@%s' % (opts.user, host), stringify_command(command)],
            stdin=subprocess.PIPE)
        proc.stdin.write(arguments)
        proc.stdin.close()
        status = proc.wait()
        if status == 0:
            break
        elif tries > 5:
            raise RuntimeError("ssh_write failed with error %s" % proc.returncode)
        else:
            print >> stderr, \
                "Error {0} while executing remote command, retrying after 30 seconds".format(status)
            time.sleep(30)
            tries = tries + 1


# Gets a list of zones to launch instances in
def get_zones(conn, opts):
    if opts.zone == 'all':
        zones = [z.name for z in conn.get_all_zones()]
    else:
        zones = [opts.zone]
    return zones


# Gets the number of items in a partition
def get_partition(total, num_partitions, current_partitions):
    num_subordinates_this_zone = total / num_partitions
    if (total % num_partitions) - current_partitions > 0:
        num_subordinates_this_zone += 1
    return num_subordinates_this_zone


def real_main():
    (opts, action, cluster_name) = parse_args()

    # Input parameter validation
    if opts.wait is not None:
        # NOTE: DeprecationWarnings are silent in 2.7+ by default.
        #       To show them, run Python with the -Wdefault switch.
        # See: https://docs.python.org/3.5/whatsnew/2.7.html
        warnings.warn(
            "This option is deprecated and has no effect. "
            "spark-ec2 automatically waits as long as necessary for clusters to startup.",
            DeprecationWarning
        )

    if opts.ebs_vol_num > 8:
        print >> stderr, "ebs-vol-num cannot be greater than 8"
        sys.exit(1)

    try:
        conn = ec2.connect_to_region(opts.region)
    except Exception as e:
        print >> stderr, (e)
        sys.exit(1)

    # Select an AZ at random if it was not specified.
    if opts.zone == "":
        opts.zone = random.choice(conn.get_all_zones()).name

    if action == "launch":
        if opts.subordinates <= 0:
            print >> sys.stderr, "ERROR: You have to start at least 1 subordinate"
            sys.exit(1)
        if opts.resume:
            (main_nodes, subordinate_nodes) = get_existing_cluster(conn, opts, cluster_name)
        else:
            (main_nodes, subordinate_nodes) = launch_cluster(conn, opts, cluster_name)
        wait_for_cluster_state(
            cluster_instances=(main_nodes + subordinate_nodes),
            cluster_state='ssh-ready',
            opts=opts
        )
        setup_cluster(conn, main_nodes, subordinate_nodes, opts, True)

    elif action == "destroy":
        print "Are you sure you want to destroy the cluster %s?" % cluster_name
        print "The following instances will be terminated:"
        (main_nodes, subordinate_nodes) = get_existing_cluster(
            conn, opts, cluster_name, die_on_error=False)
        for inst in main_nodes + subordinate_nodes:
            print "> %s" % inst.public_dns_name

        msg = "ALL DATA ON ALL NODES WILL BE LOST!!\nDestroy cluster %s (y/N): " % cluster_name
        response = raw_input(msg)
        if response == "y":
            print "Terminating main..."
            for inst in main_nodes:
                inst.terminate()
            print "Terminating subordinates..."
            for inst in subordinate_nodes:
                inst.terminate()

            # Delete security groups as well
            if opts.delete_groups:
                print "Deleting security groups (this will take some time)..."
                group_names = [cluster_name + "-main", cluster_name + "-subordinates"]
                wait_for_cluster_state(
                    cluster_instances=(main_nodes + subordinate_nodes),
                    cluster_state='terminated',
                    opts=opts
                )
                attempt = 1
                while attempt <= 3:
                    print "Attempt %d" % attempt
                    groups = [g for g in conn.get_all_security_groups() if g.name in group_names]
                    success = True
                    # Delete individual rules in all groups before deleting groups to
                    # remove dependencies between them
                    for group in groups:
                        print "Deleting rules in security group " + group.name
                        for rule in group.rules:
                            for grant in rule.grants:
                                success &= group.revoke(ip_protocol=rule.ip_protocol,
                                                        from_port=rule.from_port,
                                                        to_port=rule.to_port,
                                                        src_group=grant)

                    # Sleep for AWS eventual-consistency to catch up, and for instances
                    # to terminate
                    time.sleep(30)  # Yes, it does have to be this long :-(
                    for group in groups:
                        try:
                            conn.delete_security_group(group.name)
                            print "Deleted security group " + group.name
                        except boto.exception.EC2ResponseError:
                            success = False
                            print "Failed to delete security group " + group.name

                    # Unfortunately, group.revoke() returns True even if a rule was not
                    # deleted, so this needs to be rerun if something fails
                    if success:
                        break

                    attempt += 1

                if not success:
                    print "Failed to delete all security groups after 3 tries."
                    print "Try re-running in a few minutes."

    elif action == "login":
        (main_nodes, subordinate_nodes) = get_existing_cluster(conn, opts, cluster_name)
        main = main_nodes[0].public_dns_name
        print "Logging into main " + main + "..."
        proxy_opt = []
        if opts.proxy_port is not None:
            proxy_opt = ['-D', opts.proxy_port]
        subprocess.check_call(
            ssh_command(opts) + proxy_opt + ['-t', '-t', "%s@%s" % (opts.user, main)])

    elif action == "reboot-subordinates":
        response = raw_input(
            "Are you sure you want to reboot the cluster " +
            cluster_name + " subordinates?\n" +
            "Reboot cluster subordinates " + cluster_name + " (y/N): ")
        if response == "y":
            (main_nodes, subordinate_nodes) = get_existing_cluster(
                conn, opts, cluster_name, die_on_error=False)
            print "Rebooting subordinates..."
            for inst in subordinate_nodes:
                if inst.state not in ["shutting-down", "terminated"]:
                    print "Rebooting " + inst.id
                    inst.reboot()

    elif action == "get-main":
        (main_nodes, subordinate_nodes) = get_existing_cluster(conn, opts, cluster_name)
        print main_nodes[0].public_dns_name

    elif action == "stop":
        response = raw_input(
            "Are you sure you want to stop the cluster " +
            cluster_name + "?\nDATA ON EPHEMERAL DISKS WILL BE LOST, " +
            "BUT THE CLUSTER WILL KEEP USING SPACE ON\n" +
            "AMAZON EBS IF IT IS EBS-BACKED!!\n" +
            "All data on spot-instance subordinates will be lost.\n" +
            "Stop cluster " + cluster_name + " (y/N): ")
        if response == "y":
            (main_nodes, subordinate_nodes) = get_existing_cluster(
                conn, opts, cluster_name, die_on_error=False)
            print "Stopping main..."
            for inst in main_nodes:
                if inst.state not in ["shutting-down", "terminated"]:
                    inst.stop()
            print "Stopping subordinates..."
            for inst in subordinate_nodes:
                if inst.state not in ["shutting-down", "terminated"]:
                    if inst.spot_instance_request_id:
                        inst.terminate()
                    else:
                        inst.stop()

    elif action == "start":
        (main_nodes, subordinate_nodes) = get_existing_cluster(conn, opts, cluster_name)
        print "Starting subordinates..."
        for inst in subordinate_nodes:
            if inst.state not in ["shutting-down", "terminated"]:
                inst.start()
        print "Starting main..."
        for inst in main_nodes:
            if inst.state not in ["shutting-down", "terminated"]:
                inst.start()
        wait_for_cluster_state(
            cluster_instances=(main_nodes + subordinate_nodes),
            cluster_state='ssh-ready',
            opts=opts
        )
        setup_cluster(conn, main_nodes, subordinate_nodes, opts, False)

    else:
        print >> stderr, "Invalid action: %s" % action
        sys.exit(1)


def main():
    try:
        real_main()
    except UsageError, e:
        print >> stderr, "\nError:\n", e
        sys.exit(1)


if __name__ == "__main__":
    logging.basicConfig()
    main()
