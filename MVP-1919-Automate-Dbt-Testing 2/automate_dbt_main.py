import subprocess
import yaml
import os
import sys
import ast
from urllib.parse import urlparse

account = os.getenv("SNOWFLAKE_ACCOUNT")
host = os.getenv("SNOWFLAKE_HOST")
user = os.getenv("SNOWFLAKE_USER")
role = os.getenv("SNOWFLAKE_ROLE")
password = os.getenv("SNOWFLAKE_PASSWORD")
database = os.getenv("SNOWFLAKE_DATABASE")
git_user = os.getenv("GIT_USER")
git_password = os.getenv("GIT_PASSWORD")

def getList(dict):
    return list(dict.keys())

transformation_type = [ 'GSC','GA','GADS' ]
list1 = [ 'GSC','GA','GADS' ]

n = len(sys.argv)
# python3 script.py cust_code "['GSC','GA','GADS']" 
if n >6:
    print("ERROR: Please provide correct no of arguments")
    print(f" Arguments provided  : {sys.argv[0:]=}")
    sys.exit(1)
elif n<3:
    print("ERROR: Please provide correct no of arguments")
    print(f" Arguments provided  : {sys.argv[0:]=}")
    sys.exit(1)
else:
    print(f" Arguments provided  : {sys.argv[0:]=}")
    print(f" Running Automate DBT script")


print("Total arguments passed:", n)

print(f"Name of the script : {sys.argv[0]=}")
print(f"Provided customer code to the script : {sys.argv[1]=}")
print(f"Provided transform type  to the script : {sys.argv[2]=}")

cust_code = sys.argv[1]

print("cust_code is :",cust_code)
list2 = ast.literal_eval(sys.argv[2])
print("Transform type given : ",list2)

# using any function
out = any(check in list1 for check in list2)


def intersection(lst1, lst2):
    return list(set(lst1) & set(lst2))


# Checking condition
if out:
    transform_type=list(set(list1) & set(list2))
    print("value of transform type : ",transform_type)
    if 'GSC' in transform_type:
        GSC_TYPE='YES'
    else:
        GSC_TYPE = 'NO'

    if 'GA' in transform_type:
        GA_TYPE = 'YES'
        length = len(sys.argv) - 1
        input_schema_name_key_value_string = sys.argv[length]
        print("input_schema_name_key_value_string : ", input_schema_name_key_value_string)
        input_domain_dict = ast.literal_eval(input_schema_name_key_value_string)

        print("input_domain_dict", input_domain_dict)

        key_List = getList(input_domain_dict)
        print("the keylist: ", key_List)

        pre_hook = ''
        post_hook = ''
        string_literal = ''
        page_lookup = ''
        is_lookup_true = {}
        n = 0
        g = 0


        for key in key_List:
            schema_name = key
            print("schema_name:", key)
            domain_name_with_lookup = (input_domain_dict[key])
            print("domain_name_with_lookup:", domain_name_with_lookup)
            print("type of domain_name_with_lookup:", type(domain_name_with_lookup))
            domain_name = list(domain_name_with_lookup.keys())[0]
            print("domain_name:", domain_name)
            is_lookup = list(domain_name_with_lookup.values())[0]
            print("is_lookup:", is_lookup)
            n += 1
            if n < len(key_List):
                pre_hook += "{{{{ get_PageUserConversionPreHook_SQL('{0}', '{1}','{2}') }}}}\n    ".format(
                    cust_code, schema_name, domain_name)
                post_hook += "{{{{ get_PageUserConversionPostHook_SQL('{0}','{1}') }}}}\n    ".format(cust_code,
                                                                                                      domain_name)
                if is_lookup == 'true':
                    # is_lookup_true.append(domain_name)
                    is_lookup_true[schema_name] = domain_name
                    string_literal += " ({{{{ get_PageUserConversion_SQL('{0}', '{1}','{2}','true') }}}}) union all \n".format(
                        cust_code, schema_name, domain_name)
                else:
                    string_literal += " ({{{{ get_PageUserConversion_SQL('{0}', '{1}','{2}') }}}}) union all \n".format(
                        cust_code, schema_name, domain_name)
            else:
                pre_hook += "{{{{ get_PageUserConversionPreHook_SQL('{0}', '{1}','{2}') }}}} ".format(
                    cust_code, schema_name, domain_name)
                post_hook += "{{{{ get_PageUserConversionPostHook_SQL('{0}','{1}') }}}} ".format(cust_code, domain_name)
                if is_lookup == 'true':
                    string_literal += " ({{{{ get_PageUserConversion_SQL('{0}', '{1}','{2}','true') }}}})".format(
                        cust_code, schema_name, domain_name)
                else:
                    string_literal += " ({{{{ get_PageUserConversion_SQL('{0}', '{1}','{2}') }}}})".format(
                        cust_code, schema_name, domain_name)

        print("is_lookup_true:", is_lookup_true)

        page_lookup_key_list = getList(is_lookup_true)

        for key in is_lookup_true:
            schema_name = key
            domain_name = is_lookup_true[key]
            g += 1
            if g < len(is_lookup_true):
                page_lookup += " ({{{{ get_PageLookup_{0}_SQL('{0}', '{1}') }}}}) union all \n".format(
                    cust_code, schema_name)
            else:
                page_lookup += " ({{{{ get_PageLookup_{0}_SQL('{0}', '{1}') }}}})".format(
                    cust_code, schema_name)

        print("pre_hook:\n")
        print(pre_hook)
        print("post_hook:\n")
        print(post_hook)
        print("string_literal:\n")
        print(string_literal)

        page_user_conversion = """{{{{ config(
            materialized="incremental",
            incremental_strategy="merge",
            schema="CLICKSTREAM_ANALYTICS_{0}",
            alias="PAGE_USER_CONVERSION_UNIQUE_VISITOR",
            unique_key="uuid_string",
            pre_hook="{1}",
            post_hook="{2}" 
        )
        }}}}  

        {3}""".format(cust_code, pre_hook, post_hook, string_literal)
        # /home/automate_dbt_dir/page_user_conversion_{cust_code}.sql
        with open(f'page_user_conversion_{cust_code}.sql', 'w') as outfile:
            outfile.write(page_user_conversion)

        page_lookup = """{{{{ config(
            materialized="incremental",
            incremental_strategy="merge",
            schema="CLICKSTREAM_ANALYTICS_{0}",
            alias="PAGE_LOOKUP",
            unique_key="page"
        )
        }}}}  
        {1}""".format(cust_code, page_lookup)
        # /home/automate_dbt_dir/page_lookup_{cust_code}.sql
        with open(f'page_lookup_{cust_code}.sql', 'w') as outfile:
            outfile.write(page_lookup)

        print(" Completed successfully!!")
    else:
        GA_TYPE = 'NO'

    if 'GADS' in transform_type:
        GADS_TYPE='YES'
    else:
        GADS_TYPE = 'NO'

    print (f" we have transform type as  GSC_TYPE : {GSC_TYPE} , GA_TYPE : {GA_TYPE} and GADS_TYPE : {GADS_TYPE}")

else:
    print("FAILED: WRONG INPUT PROVIDED ")
    sys.exit(" The tranform type is not matching please provide transform type from the list : ['GSC','GA','GADS']")



profile_file = {'snowflake-dbt':{ 'target':'prod','outputs':{'prod':{'type':'snowflake','account':account,'threads':1,
                                                    'host': host,
                                                    'port':443,'user':user,
                                                    'role':role,
                                                    'password':password,
                                                    'database':database,
                                                    'schema':'QONTENT'}}}}
with open('/home/automate_dbt_dir/.dbt/profiles.yml', 'w') as outfile:
    yaml.dump(profile_file, outfile, default_flow_style=False)

print("copied profile file successfully!!")

# dbt_creation = subprocess.run(['sh', './dbt_creation.sh', 'git_user=' + str(git_user),
#                                'git_password=' + str(git_password)])

dbt_templates=subprocess.run(['bash', './dbt_creation.sh', 'git_user=' + str(git_user), 'cust_code=' +str(cust_code)
                                ,'GSC_TYPE=' +str(GSC_TYPE),'GA_TYPE=' +str(GA_TYPE),'GADS_TYPE=' +str(GADS_TYPE),
                            'git_password=' + str(git_password)])

if dbt_templates.returncode == 0:
    print(" SUCCESS : Automate dbt creation ran succeffully")
else:
    sys.exit("FAILED: Automate dbt creation has been failed. ")