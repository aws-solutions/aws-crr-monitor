#!/usr/bin/env bash

# Check to see if input has been provided:
if [ -z "$4" ]; then
    printf "Please provide the base source bucket name where the lambda code will eventually reside and version number"
    printf "For example: ./build-s3-dist.sh solutions solutions-reference v1.0 crr-monitor"
    exit 1
fi

# Build source
printf "Starting to build distribution"
printf "export deployment_dir=`pwd`"
export deployment_dir=`pwd`

printf "mkdir -p dist\n"
mkdir -p dist

# CloudFormation template creation
printf "cp -f *.template dist"
cp -f *.template dist

solution_name="crr-monitor"

#Replacing BUCKET NAME and VERSION
for replace in "s/%%DIST_BUCKET_NAME%%/$1/g" "s/%%VERSION%%/$3/g" "s/%%SOLUTION_NAME%%/${solution_name}/g"; do
    printf "sed -i '' -e $replace dist/crr-monitor.template\n"
    sed -i '' -e "${replace}" dist/crr-monitor.template
    printf "sed -i '' -e $replace dist/crr-agent.template\n"
    sed -i '' -e "${replace}" dist/crr-agent.template
done

for dir in CRRdeployagent CRRHourlyMaint CRRMonitor CRRMonitorHousekeeping CRRMonitorTrailAlarm; do
    printf "cp -r ../source/${dir} dist\n"
    cp -r ../source/"${dir}" dist
done

for dir in CRRdeployagent CRRHourlyMaint CRRMonitor CRRMonitorHousekeeping CRRMonitorTrailAlarm solution-helper; do

    printf 'cd ../source/${dir}/\n'
    cd ../source/"${dir}"/

    printf 'zip -r9 ${dir}.zip *\n'
    zip -r9 "${dir}".zip *

    printf 'mv ${dir}.zip $deployment_dir/dist\n'
    mv "${dir}".zip $deployment_dir/dist

    cd ..

done

