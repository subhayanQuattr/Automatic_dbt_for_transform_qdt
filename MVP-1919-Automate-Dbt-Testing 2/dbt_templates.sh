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
    -e=*|client_code=*)
    CLIENT_CODE="${i#*=}"
    shift
    ;;
esac
done
ls


rm -rf dbt-automate-home-dir
mkdir dbt-automate-home-dir
cd dbt-automate-home-dir

git clone https://anands-perpetualny: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@github.com/Quattr/Test_fivetran_dbt.git
#
cd Test_fivetran_dbt
#
echo " Directory is Test_fivetran_dbt "
#
#
git checkout MVP-1919-Automate-Dbt-testing
git pull origin

cp -R ./models/qdts/growthTrends/   ./models/qdts/growthTrends/$CLIENT_CODE
cp -R ./models/qdts/marketshare/   ./models/qdts/marketshare/$CLIENT_CODE
cp -R ./models/qdts/rankingFactor/ ./models/qdts/rankingFactor/$CLIENT_CODE




