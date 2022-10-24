import os

import dbt
from dbt.contracts.files import FileHash
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.model_config import NodeConfig
from dbt.config.project import PartialProject
from dbt.contracts.graph.parsed import ParsedMacro, ParsedModelNode
from dbt.node_types import NodeType


def inject_plugin(plugin):
    from dbt.adapters.factory import FACTORY
    key = plugin.adapter.type()
    FACTORY.plugins[key] = plugin


def inject_adapter(value, plugin):
    """Inject the given adapter into the adapter factory, so your hand-crafted
    artisanal adapter will be available from get_adapter() as if dbt loaded it.
    """
    inject_plugin(plugin)
    from dbt.adapters.factory import FACTORY
    key = value.type()
    FACTORY.adapters[key] = value


def empty_profile_renderer():
    import dbt
    return dbt.config.renderer.ProfileRenderer({})


def normalize(path):
    """On windows, neither is enough on its own:

    >>> normcase('C:\\documents/ALL CAPS/subdir\\..')
    'c:\\documents\\all caps\\subdir\\..'
    >>> normpath('C:\\documents/ALL CAPS/subdir\\..')
    'C:\\documents\\ALL CAPS'
    >>> normpath(normcase('C:\\documents/ALL CAPS/subdir\\..'))
    'c:\\documents\\all caps'
    """
    return os.path.normcase(os.path.normpath(path))


def project_from_dict(project, profile, packages=None, selectors=None, cli_vars='{}'):
    from dbt.context.target import generate_target_context
    from dbt.config import Project
    from dbt.config.renderer import DbtProjectYamlRenderer
    from dbt.config.utils import parse_cli_vars
    if not isinstance(cli_vars, dict):
        cli_vars = parse_cli_vars(cli_vars)

    renderer = DbtProjectYamlRenderer(profile, cli_vars)

    project_root = project.pop('project-root', os.getcwd())

    partial = PartialProject.from_dicts(
        project_root=project_root,
        project_dict=project,
        packages_dict=packages,
        selectors_dict=selectors,
    )
    return partial.render(renderer)


def profile_from_dict(profile, profile_name, cli_vars='{}'):
    from dbt.config import Profile
    from dbt.config.renderer import ProfileRenderer
    from dbt.context.base import generate_base_context
    from dbt.config.utils import parse_cli_vars
    if not isinstance(cli_vars, dict):
        cli_vars = parse_cli_vars(cli_vars)

    renderer = ProfileRenderer(cli_vars)
    return Profile.from_raw_profile_info(
        profile,
        profile_name,
        renderer,
    )


class Obj:
    which = 'blah'
    single_threaded = False


def config_from_parts_or_dicts(project, profile, packages=None, selectors=None, cli_vars='{}'):
    from dbt.config import Project, Profile, RuntimeConfig
    from copy import deepcopy

    if isinstance(project, Project):
        profile_name = project.profile_name
    else:
        profile_name = project.get('profile')

    if not isinstance(profile, Profile):
        profile = profile_from_dict(
            deepcopy(profile),
            profile_name,
            cli_vars,
        )

    if not isinstance(project, Project):
        project = project_from_dict(
            deepcopy(project),
            profile,
            packages,
            selectors,
            cli_vars,
        )

    args = Obj()
    args.vars = cli_vars
    args.profile_dir = '/dev/null'
    return RuntimeConfig.from_parts(
        project=project,
        profile=profile,
        args=args
    )


if __name__ == '__main__':
    model_config = NodeConfig.from_dict({
        'enabled': True,
        'materialized': 'view',
        'persist_docs': {},
        'post-hook': [],
        'pre-hook': [],
        'vars': {},
        'quoting': {},
        'column_types': {},
        'tags': []
    })

    project_cfg = {
        'name': 'prophecy_package',
        'version': '0.1',
        'profile': 'test',
        'project-root': '/Users/kishore/prophecy-git/jaffle_shop',
        'config-version': 2,
        'vars': {
            'test_var': 'test_var_value',
            'payment_methods': ['var_value_1', 'var_value_2']
        },
    }
    profile_cfg = {
        'outputs': {
            'test': {
                'type': 'postgres',
                'dbname': 'postgres',
                'user': 'root',
                'host': 'localhost',
                'pass': 'password',
                'port': 5432,
                'schema': 'public'
            }
            # 'test': {
            #     'type': 'bigquery',
            #     'method': 'service-account',
            #     'project': 'prophecy-development',
            #     'keyfile': '/tmp/doesnt_exist.json',
            #     'dataset': 'dataset'
            # }
        },
        'target': 'test'
    }

    config = config_from_parts_or_dicts(project_cfg, profile_cfg)
    from dbt.adapters.postgres import Plugin

    inject_adapter(Plugin.adapter(config), Plugin)
    import time

    begin = time.time()
    # projects = self.config.load_dependencies()
    # root_config = self.config
    from dbt.adapters.factory import get_adapter

    adapter = get_adapter(config)
    macro_hook = adapter.connections.set_query_header
    from dbt.parser.manifest import ManifestLoader

    macrosLoadBegin = time.time()
    loadedMacros = ManifestLoader.load_macros(config, macro_hook)
    macrosLoadEnd = time.time()
    from unit.test_parser import get_abs_os_path

    macro_root_dbt_audit = 'dbt_audit'
    customMacro = ParsedMacro(
        name=macro_root_dbt_audit,
        resource_type=NodeType.Macro,
        unique_id=macro_root_dbt_audit,
        package_name='prophecy_package',
        original_file_path=normalize('/tmp/macro.sql'),
        root_path=get_abs_os_path('/tmp/snowplow'),
        path=normalize('macros/macro.sql'),
        macro_sql='''{%- macro dbt_audit(cte_ref, created_by, updated_by, created_date, updated_date) -%}
    
        SELECT
          *,
          '{{ created_by }}'::VARCHAR       AS created_by,
          '{{ updated_by }}'::VARCHAR       AS updated_by,
          '{{ created_date }}'::DATE        AS model_created_date,
          '{{ updated_date }}'::DATE        AS model_updated_date,
          CURRENT_TIMESTAMP()               AS dbt_updated_at,
    
        {% if execute %}
    
            {% if not flags.FULL_REFRESH and config.get('materialized') == "incremental" %}
    
                {%- set source_relation = adapter.get_relation(
                    database=target.database,
                    schema=this.schema,
                    identifier=this.table,
                    ) -%}      
    
                {% if source_relation != None %}
    
                    {% set min_created_date %}
                        SELECT LEAST(MIN(dbt_created_at), CURRENT_TIMESTAMP()) AS min_ts 
                        FROM {{ this }}
                    {% endset %}
    
                    {% set results = run_query(min_created_date) %}
    
                    '{{results.columns[0].values()[0]}}'::TIMESTAMP AS dbt_created_at
    
                {% else %}
    
                    CURRENT_TIMESTAMP()               AS dbt_created_at
    
                {% endif %}
    
            {% else %}
    
                CURRENT_TIMESTAMP()               AS dbt_created_at
    
            {% endif %}
        
        {% endif %}
    
        FROM {{ cte_ref }}
    
    {%- endmacro -%}
    ''',
    )
    customMacros = {macro_root_dbt_audit: customMacro}
    print(f"Total time in macros ({macrosLoadEnd - macrosLoadBegin})")
    # loader = ManifestLoader(root_config, projects, macro_hook)
    # newManifest = loader.load()
    manifest = Manifest(
        macros=loadedMacros.macros | customMacros,
        nodes={
            'test': ParsedModelNode(
                name='stg_payments',
                database='dbt',
                schema='analytics',
                alias='stg_payments',
                resource_type=NodeType.Model,
                unique_id='stg_payments',
                fqn=['stg_payments'],
                package_name='prophecy_package',
                root_path='/usr/src/app',
                config=model_config,
                path='view.sql',
                original_file_path='view.sql',
                language='sql',
                # raw_code="",
                raw_code='''''',
                checksum=FileHash.from_contents(''),
            ),
            # 'model.root.ephemeral': ParsedModelNode(
            #     name='ephemeral',
            #     database='dbt',
            #     schema='analytics',
            #     alias='view',
            #     resource_type=NodeType.Model,
            #     unique_id='model.root.ephemeral',
            #     fqn=['root', 'ephemeral'],
            #     package_name='root',
            #     root_path='/usr/src/app',
            #     config=ephemeral_config,
            #     path='ephemeral.sql',
            #     original_file_path='ephemeral.sql',
            #     # language='sql',
            #     # raw_code='select * from source_table',
            #     raw_sql='',
            #     path='stg_payments.sql',
            #     original_file_path='stg_payments.sql',
            #     # raw_sql='''select 1''',
            #     checksum=FileHash.from_contents(''),
            #     # depends_on=MacroDependsOn(),
            # ),
        },
        sources={},
        docs={},
        disabled=[],
        files={},
        exposures={},
        metrics={},
        selectors={},
    )
    from dbt.context.providers import generate_parser_model_context
    from dbt.context.context_config import ContextConfig

    contextCreation = time.time()
    config.vars.vars = {'test_var': 'test_var_value_wefe', 'payment_methods': ['var_value_1', 'var_value_2']}
    # config.vars =
    # newConfig = config_from_parts_or_dicts(self.project_cfg, self.profile_cfg)
    ret = generate_parser_model_context(manifest.nodes['test'], config, manifest, ContextConfig(
        config,
        manifest.nodes['test'].fqn,
        manifest.nodes['test'].resource_type,
        "root",
    ))

    from dbt.clients.jinja import get_template

    end1 = time.time()
    print(f"Total time in contextCreation ({end1 - contextCreation})")
    query = """
    {{ config({         
        "materialized": "incremental",         
        "unique_key": "order_id",         
        "tags": ["orders_snapshots"],         
        "alias": "orders"     
        }) 
    }} 
    {% set payment_methods = ['credit_card', 'coupon', 'bank_transfer', 'gift_card', 1] %}  
    {% set my_abc = 'abc' %}  
    with orders as ( select *, {{var('test_var')}} from {{ ref('stg_orders') }}  ), 
    payments as (      select * from {{ ref('stg_payments') }}  ),  
    order_payments as (
        select     
            order_id,
            {{my_abc}},        
            {{payment_methods}},        
            {% for payment_method in payment_methods -%}     
                sum(case when payment_method = '{{ payment_method }}' then amount else 0 end) as {{ payment_method }}_amount,         
            {% endfor -%}          
            sum(amount) as total_amount      
        from payments      
        group by order_id  
    ),  
    final as (      
        select    
             orders.order_id,         
             orders.customer_id,         
             orders.order_date,         
             orders.status,          
             {% for payment_method in var("payment_methods") -%}     
                  order_payments.{{ payment_method }}_amount,          
             {% endfor -%}          
             order_payments.total_amount as amount      
        from orders       left join order_payments         on orders.order_id = order_payments.order_id  
    )  
    select * from final 
    {% if is_incremental() %}    -- this filter will only be applied on an incremental run   
        where order_date > (select max(order_date) from {{ this }})  
    {% endif %}
    {{ dbt_utils.group_by(2) }}
    {{ dbt_audit(
        cte_ref="final",
        created_by="@kishore",
        updated_by="@bandi",
        created_date="2021-06-02",
        updated_date="2022-04-04"
    ) }}"""

    newVal = get_template(query, ret,
                          capture_macros=False).module  # .make_module(vars={'test_var': 'test_var_value_wefe'})
    end2 = time.time()
    print(f"Total time ({end2 - begin})")
    print(f"Total time since contextCreation ({end2 - contextCreation})")
    print(newVal)
