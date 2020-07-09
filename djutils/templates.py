import datajoint as dj
import inspect


def required(f):
    f.required = True
    return f


def optional(f):
    f.optional = True
    return f


class SchemaTemplate:

    """
    A schema object is a decorator for datajoint pipeline classes
    """

    def __init__(self, context=None):
        self._context = context or inspect.currentframe().f_back.f_locals

        self.upstream_table_names = []
        self.required_method_names = []
        self.optional_method_names = []
        self._table_classes = {}
        self.schema = None

    def list_requirements(self):
        """
        return: a message as a guidance for users to generate proper requirements
        """
        if not (self.upstream_table_names or self.required_method_names or self.optional_method_names):
            print('No required upstream tables or methods.')

        msg = '"requirements" needs to be a dictionary with:\n'

        if self.upstream_table_names:
            msg += '\tKeys for upstream tables: {}\n'.format(self.upstream_table_names)
        if self.required_method_names:
            msg += '\tKeys for require methods: {}\n'.format(self.required_method_names)
        if self.optional_method_names:
            msg += '\tKeys for optional methods: {}\n'.format(self.optional_method_names)

        print(msg)

    def _check_dependencies(self, dependencies):
        valid_dependencies = {}
        if self.upstream_table_names:
            for k in self.upstream_table_names:
                if k not in dependencies:
                    raise KeyError('Requiring upstream table: {}'.format(k))
                else:
                    valid_dependencies[k] = dependencies[k]

        if self.required_method_names:
            for k in self.required_method_names:
                if k not in dependencies or not inspect.isfunction(dependencies[k]):
                    raise KeyError('Requiring method: {}'.format(k))
                else:
                    valid_dependencies[k] = dependencies[k]

        return valid_dependencies

    def list_tables(self):
        for tbl in self._table_classes:
            print(tbl.__name__)

    def __call__(self, table_class):
        '''
        While decorating, add table classes into self._table_classes
        '''

        if table_class in self._table_classes:
            raise RuntimeError('Duplicated table: {}'.format(table_class.__name__))

        # check for required_table_names
        upstream_table_names = [str(k)[1:] for k, v in vars(table_class).items()
                                if k.startswith('_') and v == Ellipsis]
        # check for required_method_names
        required_method_names = [str(k)[1:] for k in vars(table_class)
                                 if k.startswith('_') and getattr(getattr(table_class, k), 'required', False)]
        # check for optional_method_names
        optional_method_names = [str(k)[1:] for k in vars(table_class)
                                 if k.startswith('_') and getattr(getattr(table_class, k), 'optional', False)]

        self.upstream_table_names.extend(n for n in upstream_table_names if n not in self.upstream_table_names)
        self.required_method_names.extend(n for n in required_method_names if n not in self.required_method_names)
        self.optional_method_names.extend(n for n in optional_method_names if n not in self.optional_method_names)

        self._table_classes[table_class] = {'upstreams_tbls': upstream_table_names,
                                            'required_methods': required_method_names,
                                            'optional_methods': optional_method_names}

        return table_class

    def declare(self, schema, dependencies=None, context=None):
        """
        Method to declare tables in a datajoint pipeline in a schema
        :param schema: a string for schema name OR the schema object to decorate this pipeline
        :param dependencies: a dictionary listing required tables and required methods
        :param context: dictionary for looking up foreign key references, leave None to use local context.
        """
        if self.schema is not None:
            raise RuntimeError('Unable to initialize this template schema twice!')

        if isinstance(schema, str):
            schema = dj.schema(schema)

        dependencies = self._check_dependencies(dependencies)

        if not context:
            context = inspect.currentframe().f_back.f_locals

        self._context.update(**context)

        for table_class, tbl_reqs in self._table_classes.items():
            for required_attr in tbl_reqs['upstreams_tbls'] + tbl_reqs['required_methods'] + tbl_reqs['optional_methods']:
                hook_target = dependencies.get(required_attr, _undefined_optional_method)
                hook_name = '_{}'.format(required_attr)
                setattr(table_class, hook_name, hook_target)

            print('Initializing {}'.format(table_class.__name__))
            table = schema(table_class, context=self._context)
            self._context[table.__name__] = table
            setattr(self, table.__name__, table)

        self.schema = schema


def _undefined_optional_method(*args, **kwargs):
    raise NotImplementedError
