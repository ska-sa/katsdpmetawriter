#!groovy

@Library('katsdpjenkins') _

katsdp.killOldJobs()
katsdp.setDependencies(['ska-sa/katsdpdockerbase/master',
                        'ska-sa/katsdpservices/master',
                        'ska-sa/katsdptelstate/master'])
katsdp.standardBuild(python3: true, python2: false, push_external: true)
katsdp.mail('sdpdev+katsdpmetawriter@ska.ac.za')
