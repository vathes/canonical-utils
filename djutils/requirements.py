import inspect

class Requirements:

    def __init__(self, required_tbl_names=None, required_method_names=None):

        self.tables = required_tbl_names
        self.methods = required_method_names

    def __call__(self):

        if not (self.tables or self.methods):
            return 'No required upstream tables or methods.'

        msg = u'"requirements" needs to be a dictionary with'

        if self.tables:
            msg += u' - Keys for upstream tables: {}'.format(self.tables)
        if self.methods:
            msg += u' - Keys for require methods: {}'.format(self.methods)

        return msg

    def check_requirements(self, requirements=None):
        if not requirements:
            raise KeyError(self.__call__())

        checked_requirements = {}
        for k in self.tables:
            if k not in requirements:
                raise KeyError(f'Requiring upstream table: {k}')
            else:
                checked_requirements[k] = requirements[k]

        if self.methods:
            for k in self.methods:
                if k not in requirements or not inspect.isfunction(requirements[k]):
                    raise KeyError(f'Requiring method: {k}')
                else:
                    checked_requirements[k] = requirements[k]

        return checked_requirements
