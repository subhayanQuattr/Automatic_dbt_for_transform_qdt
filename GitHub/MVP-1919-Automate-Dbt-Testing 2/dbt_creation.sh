#!/bin/bash
for i in "$@"
do
case $i in
     -s=*|git_user=*)
    GIT_USER="${i#*=}"
    shift
    ;;
    -d=*|git_password=*)
    GIT_PASSWORD="${i#*=}"
    shift
    ;;
    -e=*|cust_code=*)
    cust_code="${i#*=}"
    shift
    ;;
    -s=*|GSC_TYPE=*)
    GSC_TYPE="${i#*=}"
    shift
    ;;
    -t=*|GA_TYPE=*)
    GA_TYPE="${i#*=}"
    shift
    ;;
    -p=*|GADS_TYPE=*)
    GADS_TYPE="${i#*=}"
    shift
    ;;
esac
done
ls
#cp profiles.yml  .dbt/.

echo "cust_code = ${cust_code}"

echo "GSC_TYPE = ${GSC_TYPE}"
echo "GA_TYPE = ${GA_TYPE}"
echo "GADS_TYPE = ${GADS_TYPE}"

rm -rf dbt-automate-home-dir
mkdir dbt-automate-home-dir
cd dbt-automate-home-dir

git clone https://${GIT_USER}:${GIT_PASSWORD}@github.com/Quattr/Test_fivetran_dbt.git
#
cd Test_fivetran_dbt
#
echo " Directory is Test_fivetran_dbt "
#
#
git checkout MVP-1919-Automate-Dbt-testing
git pull origin
echo " Started working on dbt "

echo $PWD

source='ADBE'
# cust_code='KING'
list=('growthTrends'  'rankingFactor' 'marketshare')
#list=('growthTrends')



#mkdir -p ./models/qdts/rankingFactor/$cust_code
#cp -R  ./models/qdts/rankingFactor/$source/.   ./models/qdts/rankingFactor/$cust_code/.

for dir in "${list[@]}"
do
  echo $PWD
  echo "source from which we are copying: $source"
  echo "target to which we are copying model files :$cust_code"
  echo $dir
  mkdir -p ./models/qdts/$dir/$cust_code

  cp -R  ./models/qdts/$dir/$source/.   ./models/qdts/$dir/$cust_code/.
  echo "AFTER COPY"
  echo $PWD
  cd models/qdts/$dir/$cust_code
  echo "INSIDE DIRECTORY"
  echo $PWD
#
for file in $(find . -name '*.sql')
do
  mv $file $(echo "$file" | sed -r "s/$source/$cust_code/g")
done

find . -type f -exec sed -i "s/$source/$cust_code/g" {} +
#
  cd ../../../..
done
ls




GSC='YES'
GA='YES'
GADS='YES'
##
mkdir -p ./models/transforms/$cust_code

if [  "$GSC_TYPE"  = "$GSC" ]
then
    echo "We have given GSC Type as yes so copying google search console transforms"
    cp -R  ./models/transforms/ADBE/.   ./models/transforms/$cust_code/.
    cd ./models/transforms/$cust_code
    for filename in *; do mv "$filename" "${filename//ADBE/$cust_code}"; done
    sed -i -- "s/ADBE/$cust_code/g" *
    cd ../../..
else
    echo " GSC type is NO so not copying gsc transforms "
fi
#
#ls
#echo $PWD
#
#if [  "$GA_TYPE"  = "$GA" ]
#then
#    echo "We have given GA_TYPE Type as yes so copying google analytics transforms"
#    cp -R  ./models/transforms/NVSP/page_*.sql   ./models/transforms/$cust_code/.
#    cd ./models/transforms/$cust_code
#    for filename in *; do mv "$filename" "${filename//NVSP/$cust_code}"; done
#    sed -i -- "s/NVSP/$cust_code/g" *
#    cd ../../..
#else
#    echo " GA_TYPE type is NO so not copying oogle analytics transforms "
#fi
#
if [  "$GADS_TYPE"  = "$GADS" ]
then
    echo "We have given GADS Type as yes so copying google search console transforms"
    cp -R  ./models/transforms/POCK/keyword_page_paid_stats_POCK.sql  ./models/transforms/$cust_code/.
    cd ./models/transforms/$cust_code
    for filename in *; do mv "$filename" "${filename//POCK/$cust_code}"; done
    sed -i -- "s/POCK/$cust_code/g" *
    cd ../../..
else
    echo " GADS type is NO so not copying google ads transforms "
fi

 if [  "$GA_TYPE"  = "$GA" ]
 then
    echo "We have given GA_TYPE Type as yes so copying google analytics transforms"
    cp -R  /home/automate_dbt_dir/page_*   ./models/transforms/$cust_code/.
#    cp -R  /home/automate_dbt_dir/page_lookup_NVSP.sql   ./models/transforms/$cust_code/.

 else
    echo " GA_TYPE type is NO so not copying google analytics transforms "
 fi



#*******************************************************
#ls
# dbt debug
#--profiles-dir /home/myname/.dbt
#dbt debug
#dbt deps --profiles-dir .
#dbt debug --profiles-dir .
# --profiles-dir /home/myname/.dbt
#
#dbt debug --profiles-dir /root/.dbt/
#
#
#dbt debug --profiles-dir .
#
#dbt debug --profiles-dir /home/automate_dbt_dir/.dbt/
#dbt debug --config-dir
#dbt run --models models/qdts/rankingFactor/LAF/ranking_factor_LAF.sql
#****************************************************************************


# dbt run --profiles-dir /home/automate_dbt_dir/.dbt/ --models models/qdts/rankingFactor/$cust_code/*
# dbt debug --config-dir
# dbt run --models models/qdts/rankingFactor/$cust_code/*

dbt run --profiles-dir /home/automate_dbt_dir/.dbt/ --models models/qdts/growthTrends/$cust_code/* models/qdts/rankingFactor/$cust_code/* models/qdts/marketshare/$cust_code/*
# dbt run --profiles-dir  --models models/qdts/growthTrends/$cust_code/* models/qdts/rankingFactor/$cust_code/* models/qdts/marketshare/$cust_code/*

if [ $? -eq 0 ]; then
    echo " DBT model qdts run successfully"
else
   echo "Dbt model qdts run failed"
   exit 1
fi

dbt run --profiles-dir /home/automate_dbt_dir/.dbt/ --models models/transforms/$cust_code/*
 if [ $? -eq 0 ]; then
     echo " DBT transforms run successful"
     git add .
     git commit -m "MVP-1919: Automate dbt creation for customer "
     git pull origin
     git push
 else
    echo "Dbt transforms run failed"
    exit 1
 fi

# git add .
# git commit -m "MVP-1919: Automate dbt creation for customer "
# # git pull origin
# git push
