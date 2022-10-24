import unittest
from unittest.mock import MagicMock, patch

import dbt.flags
import dbt.compilation
from dbt.adapters.postgres import Plugin
from dbt.contracts.files import FileHash
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.parsed import NodeConfig, DependsOn, ParsedModelNode, MacroDependsOn, ParsedMacro
from dbt.contracts.graph.compiled import CompiledModelNode, InjectedCTE
from dbt.node_types import NodeType

from .utils import inject_adapter, clear_plugin, config_from_parts_or_dicts, normalize


class CompilerTest(unittest.TestCase):
    def assertEqualIgnoreWhitespace(self, a, b):
        self.assertEqual(
            "".join(a.split()),
            "".join(b.split()))

    def setUp(self):
        self.maxDiff = None

        self.model_config = NodeConfig.from_dict({
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
                # 'test': {
                #     'type': 'postgres',
                #     'dbname': 'postgres',
                #     'user': 'root',
                #     'host': 'localhost',
                #     'pass': 'password',
                #     'port': 5432,
                #     'schema': 'public'
                # }
                'test': {
                    'type': 'bigquery',
                    'method': 'service-account',
                    'project': 'prophecy-development',
                    'keyfile': '/tmp/doesnt_exist.json',
                    'dataset': 'dataset'
                }
            },
            'target': 'test'
        }

        self.config = config_from_parts_or_dicts(project_cfg, profile_cfg)

        self._generate_runtime_model_context_patch = patch.object(
            dbt.compilation, 'generate_runtime_model_context')
        self.mock_generate_runtime_model_context = self._generate_runtime_model_context_patch.start()

        inject_adapter(Plugin.adapter(self.config), Plugin)

        def mock_generate_runtime_model_context_meth(model, config, manifest):
            def ref(name):
                result = f'__dbt__cte__{name}'
                unique_id = f'model.prophecy_package.{name}'
                model.extra_ctes.append(InjectedCTE(id=unique_id, sql=None))
                return result

            return {'ref': ref}

        self.mock_generate_runtime_model_context.side_effect = mock_generate_runtime_model_context_meth

    def tearDown(self):
        self._generate_runtime_model_context_patch.stop()
        clear_plugin(Plugin)

    def test__prepend_ctes__already_has_cte(self):
        ephemeral_config = self.model_config.replace(materialized='ephemeral')

        import time
        begin = time.time()
        # projects = self.config.load_dependencies()
        # root_config = self.config
        from dbt.adapters.factory import get_adapter
        adapter = get_adapter(self.config)
        macro_hook = adapter.connections.set_query_header
        from dbt.parser.manifest import ManifestLoader
        macrosLoadBegin = time.time()
        loadedMacros = ManifestLoader.load_macros(self.config, macro_hook)
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
                    config=self.model_config,
                    path='view.sql',
                    original_file_path='view.sql',
                    # language='sql',
                    # raw_code="",
                    raw_sql='''{% set payment_methods = ['credit_card', 'coupon', 'bank_transfer', 'gift_card', 1] %} {% set my_abc = 'abc' %} with cte as (select * from something_else) select *, {{var("test_var")}},{% for payment_method in payment_methods -%} sum(case when payment_method = '{{ payment_method }}' then amount else 0 end) as {{ payment_method }}_amount,         {% endfor -%} {{my_abc}},         {{payment_methods}}, last_name from {{ref("ephemeral")}}''',
                    checksum=FileHash.from_contents(''),
                ),
                'model.root.ephemeral': ParsedModelNode(
                    name='ephemeral',
                    database='dbt',
                    schema='analytics',
                    alias='view',
                    resource_type=NodeType.Model,
                    unique_id='model.root.ephemeral',
                    fqn=['root', 'ephemeral'],
                    package_name='root',
                    root_path='/usr/src/app',
                    config=ephemeral_config,
                    path='ephemeral.sql',
                    original_file_path='ephemeral.sql',
                    # language='sql',
                    # raw_code='select * from source_table',
                    raw_sql='',
                    path='stg_payments.sql',
                    original_file_path='stg_payments.sql',
                    raw_sql='''select 1''',
                    checksum=FileHash.from_contents(''),
                    # depends_on=MacroDependsOn(),
                ),
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
        # newConfig = config_from_parts_or_dicts(self.project_cfg, self.profile_cfg)
        ret = generate_parser_model_context(manifest.nodes['test'], self.config, manifest, ContextConfig(
            self.config,
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
        newVal = get_template(query, ret, capture_macros=False).make_module(vars={'test_var': 'test_var_value_wefe'})
        end2 = time.time()
        print(f"Total time ({end2 - begin})")
        print(newVal)

        compiler = dbt.compilation.Compiler(self.config)
        result = compiler.compile_node(
            manifest.nodes['model.root.view'], manifest, write=False)

        self.assertEqual(result, manifest.nodes['model.root.view'])
        self.assertEqual(result.extra_ctes_injected, True)
        self.assertEqualIgnoreWhitespace(
            result.compiled_code,
            ('with __dbt__cte__ephemeral as ('
             'select * from source_table'
             '), cte as (select * from something_else) '
             'select * from __dbt__cte__ephemeral'))
        self.assertEqual(
            manifest.nodes['model.root.ephemeral'].extra_ctes_injected,
            True)

    def test__prepend_ctes__no_ctes(self):
        manifest = Manifest(
            macros={},
            nodes={
                'model.root.view': ParsedModelNode(
                    name='view',
                    database='dbt',
                    schema='analytics',
                    alias='view',
                    resource_type=NodeType.Model,
                    unique_id='model.root.view',
                    fqn=['root', 'view'],
                    package_name='root',
                    root_path='/usr/src/app',
                    config=self.model_config,
                    path='view.sql',
                    original_file_path='view.sql',
                    language='sql',
                    raw_code=('with cte as (select * from something_else) '
                             'select * from source_table'),
                    checksum=FileHash.from_contents(''),
                ),
                'model.root.view_no_cte': ParsedModelNode(
                    name='view_no_cte',
                    database='dbt',
                    schema='analytics',
                    alias='view_no_cte',
                    resource_type=NodeType.Model,
                    unique_id='model.root.view_no_cte',
                    fqn=['root', 'view_no_cte'],
                    package_name='root',
                    root_path='/usr/src/app',
                    config=self.model_config,
                    path='view.sql',
                    original_file_path='view.sql',
                    language='sql',
                    raw_code='select * from source_table',
                    checksum=FileHash.from_contents(''),
                ),
            },
            sources={},
            docs={},
            disabled=[],
            files={},
            exposures={},
            metrics={},
            selectors={},
        )

        compiler = dbt.compilation.Compiler(self.config)
        result = compiler.compile_node(
            manifest.nodes['model.root.view'], manifest, write=False)

        self.assertEqual(
            result,
            manifest.nodes.get('model.root.view'))
        self.assertTrue(result.extra_ctes_injected)
        self.assertEqualIgnoreWhitespace(
            result.compiled_code,
            ('with cte as (select * from something_else) '
             'select * from source_table')
        )

        compiler = dbt.compilation.Compiler(self.config)
        result = compiler.compile_node(
            manifest.nodes['model.root.view_no_cte'], manifest, write=False)

        self.assertEqual(
            result,
            manifest.nodes.get('model.root.view_no_cte'))
        self.assertTrue(result.extra_ctes_injected)
        self.assertEqualIgnoreWhitespace(
            result.compiled_code,
            'select * from source_table'
        )

    def test__prepend_ctes(self):
        ephemeral_config = self.model_config.replace(materialized='ephemeral')

        manifest = Manifest(
            macros={},
            nodes={
                'model.root.view': ParsedModelNode(
                    name='view',
                    database='dbt',
                    schema='analytics',
                    alias='view',
                    resource_type=NodeType.Model,
                    unique_id='model.root.view',
                    fqn=['root', 'view'],
                    package_name='root',
                    root_path='/usr/src/app',
                    config=self.model_config,
                    path='view.sql',
                    original_file_path='view.sql',
                    language='sql',
                    raw_code='select * from {{ref("ephemeral")}}',
                    checksum=FileHash.from_contents(''),
                ),
                'model.root.ephemeral': ParsedModelNode(
                    name='ephemeral',
                    database='dbt',
                    schema='analytics',
                    alias='ephemeral',
                    resource_type=NodeType.Model,
                    unique_id='model.root.ephemeral',
                    fqn=['root', 'ephemeral'],
                    package_name='root',
                    root_path='/usr/src/app',
                    config=ephemeral_config,
                    path='ephemeral.sql',
                    original_file_path='ephemeral.sql',
                    language='sql',
                    raw_code='select * from source_table',
                    checksum=FileHash.from_contents(''),
                ),
            },
            sources={},
            docs={},
            disabled=[],
            files={},
            exposures={},
            metrics={},
            selectors={},
        )

        compiler = dbt.compilation.Compiler(self.config)
        result = compiler.compile_node(
            manifest.nodes['model.root.view'],
            manifest,
            write=False
        )
        self.assertTrue(result.extra_ctes_injected)
        self.assertEqualIgnoreWhitespace(
            result.compiled_code,
            ('with __dbt__cte__ephemeral as ('
             'select * from source_table'
             ') '
             'select * from __dbt__cte__ephemeral'))
        self.assertTrue(
            manifest.nodes['model.root.ephemeral'].extra_ctes_injected)

    def test__prepend_ctes__cte_not_compiled(self):
        ephemeral_config = self.model_config.replace(materialized='ephemeral')
        parsed_ephemeral = ParsedModelNode(
            name='ephemeral',
            database='dbt',
            schema='analytics',
            alias='ephemeral',
            resource_type=NodeType.Model,
            unique_id='model.root.ephemeral',
            fqn=['root', 'ephemeral'],
            package_name='root',
            root_path='/usr/src/app',
            refs=[],
            sources=[],
            depends_on=DependsOn(),
            config=ephemeral_config,
            tags=[],
            path='ephemeral.sql',
            original_file_path='ephemeral.sql',
            language='sql',
            raw_code='select * from source_table',
            checksum=FileHash.from_contents(''),
        )
        compiled_ephemeral = CompiledModelNode(
            name='ephemeral',
            database='dbt',
            schema='analytics',
            alias='ephemeral',
            resource_type=NodeType.Model,
            unique_id='model.root.ephemeral',
            fqn=['root', 'ephemeral'],
            package_name='root',
            root_path='/usr/src/app',
            refs=[],
            sources=[],
            depends_on=DependsOn(),
            config=ephemeral_config,
            tags=[],
            path='ephemeral.sql',
            original_file_path='ephemeral.sql',
            language='sql',
            raw_code='select * from source_table',
            compiled=True,
            compiled_code='select * from source_table',
            extra_ctes_injected=True,
            extra_ctes=[],
            checksum=FileHash.from_contents(''),
        )
        manifest = Manifest(
            macros={},
            nodes={
                'model.root.view': CompiledModelNode(
                    name='view',
                    database='dbt',
                    schema='analytics',
                    alias='view',
                    resource_type=NodeType.Model,
                    unique_id='model.root.view',
                    fqn=['root', 'view'],
                    package_name='root',
                    root_path='/usr/src/app',
                    refs=[],
                    sources=[],
                    depends_on=DependsOn(nodes=['model.root.ephemeral']),
                    config=self.model_config,
                    tags=[],
                    path='view.sql',
                    original_file_path='view.sql',
                    language='sql',
                    raw_code='select * from {{ref("ephemeral")}}',
                    compiled=True,
                    extra_ctes_injected=False,
                    extra_ctes=[InjectedCTE(
                        id='model.root.ephemeral', sql='select * from source_table')],
                    compiled_code='select * from __dbt__cte__ephemeral',
                    checksum=FileHash.from_contents(''),
                ),
                'model.root.ephemeral': parsed_ephemeral,
            },
            sources={},
            docs={},
            disabled=[],
            files={},
            exposures={},
            metrics={},
            selectors={},
        )

        compiler = dbt.compilation.Compiler(self.config)
        with patch.object(compiler, '_compile_node') as compile_node:
            compile_node.return_value = compiled_ephemeral

            result, _ = compiler._recursively_prepend_ctes(
                manifest.nodes['model.root.view'],
                manifest,
                {}
            )
            compile_node.assert_called_once_with(
                parsed_ephemeral, manifest, {})

        self.assertEqual(result,
                         manifest.nodes.get('model.root.view'))

        self.assertTrue(manifest.nodes['model.root.ephemeral'].compiled)
        self.assertTrue(result.extra_ctes_injected)
        self.assertEqualIgnoreWhitespace(
            result.compiled_code,
            ('with __dbt__cte__ephemeral as ('
             'select * from source_table'
             ') '
             'select * from __dbt__cte__ephemeral'))

        self.assertTrue(
            manifest.nodes['model.root.ephemeral'].extra_ctes_injected)

    def test__prepend_ctes__multiple_levels(self):
        ephemeral_config = self.model_config.replace(materialized='ephemeral')

        manifest = Manifest(
            macros={},
            nodes={
                'model.root.view': ParsedModelNode(
                    name='view',
                    database='dbt',
                    schema='analytics',
                    alias='view',
                    resource_type=NodeType.Model,
                    unique_id='model.root.view',
                    fqn=['root', 'view'],
                    package_name='root',
                    root_path='/usr/src/app',
                    config=self.model_config,
                    path='view.sql',
                    original_file_path='view.sql',
                    language='sql',
                    raw_code='select * from {{ref("ephemeral")}}',
                    checksum=FileHash.from_contents(''),

                ),
                'model.root.ephemeral': ParsedModelNode(
                    name='ephemeral',
                    database='dbt',
                    schema='analytics',
                    alias='ephemeral',
                    resource_type=NodeType.Model,
                    unique_id='model.root.ephemeral',
                    fqn=['root', 'ephemeral'],
                    package_name='root',
                    root_path='/usr/src/app',
                    config=ephemeral_config,
                    path='ephemeral.sql',
                    original_file_path='ephemeral.sql',
                    language='sql',
                    raw_code='select * from {{ref("ephemeral_level_two")}}',
                    checksum=FileHash.from_contents(''),
                ),
                'model.root.ephemeral_level_two': ParsedModelNode(
                    name='ephemeral_level_two',
                    database='dbt',
                    schema='analytics',
                    alias='ephemeral_level_two',
                    resource_type=NodeType.Model,
                    unique_id='model.root.ephemeral_level_two',
                    fqn=['root', 'ephemeral_level_two'],
                    package_name='root',
                    root_path='/usr/src/app',
                    config=ephemeral_config,
                    path='ephemeral_level_two.sql',
                    original_file_path='ephemeral_level_two.sql',
                    language='sql',
                    raw_code='select * from source_table',
                    checksum=FileHash.from_contents(''),
                ),
            },
            sources={},
            docs={},
            disabled=[],
            files={},
            exposures={},
            metrics={},
            selectors={},
        )

        compiler = dbt.compilation.Compiler(self.config)
        result = compiler.compile_node(manifest.nodes['model.root.view'], manifest, write=False)

        self.assertEqual(result, manifest.nodes['model.root.view'])
        self.assertTrue(result.extra_ctes_injected)
        self.assertEqualIgnoreWhitespace(
            result.compiled_code,
            ('with __dbt__cte__ephemeral_level_two as ('
             'select * from source_table'
             '), __dbt__cte__ephemeral as ('
             'select * from __dbt__cte__ephemeral_level_two'
             ') '
             'select * from __dbt__cte__ephemeral'))

        self.assertTrue(manifest.nodes['model.root.ephemeral'].compiled)
        self.assertTrue(
            manifest.nodes['model.root.ephemeral_level_two'].compiled)
        self.assertTrue(
            manifest.nodes['model.root.ephemeral'].extra_ctes_injected)
        self.assertTrue(
            manifest.nodes['model.root.ephemeral_level_two'].extra_ctes_injected)

    def test__prepend_ctes__valid_ephemeral_sql(self):
        """Assert that the compiled sql for ephemeral models is valid and can be executed on its own"""
        ephemeral_config = self.model_config.replace(materialized='ephemeral')

        manifest = Manifest(
            macros={},
            nodes={
                'model.root.view': ParsedModelNode(
                    name='view',
                    database='dbt',
                    schema='analytics',
                    alias='view',
                    resource_type=NodeType.Model,
                    unique_id='model.root.view',
                    fqn=['root', 'view'],
                    package_name='root',
                    root_path='/usr/src/app',
                    config=self.model_config,
                    path='view.sql',
                    original_file_path='view.sql',
                    language='sql',
                    raw_code='select * from {{ref("ephemeral")}}',
                    checksum=FileHash.from_contents(''),
                ),
                'model.root.inner_ephemeral': ParsedModelNode(
                    name='inner_ephemeral',
                    database='dbt',
                    schema='analytics',
                    alias='inner_ephemeral',
                    resource_type=NodeType.Model,
                    unique_id='model.root.inner_ephemeral',
                    fqn=['root', 'inner_ephemeral'],
                    package_name='root',
                    root_path='/usr/src/app',
                    config=ephemeral_config,
                    path='inner_ephemeral.sql',
                    original_file_path='inner_ephemeral.sql',
                    language='sql',
                    raw_code='select * from source_table',
                    checksum=FileHash.from_contents(''),
                ),
                'model.root.ephemeral': ParsedModelNode(
                    name='ephemeral',
                    database='dbt',
                    schema='analytics',
                    alias='ephemeral',
                    resource_type=NodeType.Model,
                    unique_id='model.root.ephemeral',
                    fqn=['root', 'ephemeral'],
                    package_name='root',
                    root_path='/usr/src/app',
                    config=ephemeral_config,
                    path='ephemeral.sql',
                    original_file_path='ephemeral.sql',
                    language='sql',
                    raw_code='select * from {{ ref("inner_ephemeral") }}',
                    checksum=FileHash.from_contents(''),
                ),
            },
            sources={},
            docs={},
            disabled=[],
            files={},
            exposures={},
            metrics={},
            selectors={},
        )

        compiler = dbt.compilation.Compiler(self.config)
        result = compiler.compile_node(
            manifest.nodes['model.root.view'],
            manifest,
            write=False
        )
        self.assertEqualIgnoreWhitespace(
            result.compiled_code,
            ('with __dbt__cte__inner_ephemeral as ('
             'select * from source_table'
             '), '
             '__dbt__cte__ephemeral as ('
             'select * from __dbt__cte__inner_ephemeral'
             ') '
             'select * from __dbt__cte__ephemeral'))
        self.assertEqualIgnoreWhitespace(
            manifest.nodes['model.root.ephemeral'].compiled_code,
            ('with __dbt__cte__inner_ephemeral as ('
             'select * from source_table'
             ')  '
             'select * from __dbt__cte__inner_ephemeral')
        )
