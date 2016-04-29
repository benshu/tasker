class Connector:
    '''
    '''
    name = ''

    def pop(self, key, timeout=0):
        '''
        '''
        raise NotImplemented()

    def pop_bulk(self, key, count):
        '''
        '''
        raise NotImplemented()

    def push(self, key, value):
        '''
        '''
        raise NotImplemented()

    def push_bulk(self, key, values):
        '''
        '''
        raise NotImplemented()

    def add_to_set(self, set_name, value):
        '''
        '''
        raise NotImplemented()

    def remove_from_set(self, set_name, value):
        '''
        '''
        raise NotImplemented()

    def is_member_of_set(self, set_name, value):
        '''
        '''
        raise NotImplemented()

    def len(self, key):
        '''
        '''
        raise NotImplemented()

    def delete(self, key):
        '''
        '''
        raise NotImplemented()

    def __getstate__(self):
        '''
        '''
        raise NotImplemented()

    def __setstate__(self, value):
        '''
        '''
        raise NotImplemented()
