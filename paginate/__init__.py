from restkit import BasicAuth
import logging

from django.conf import settings

from couchdbkit import schema
from couchdbkit.exceptions import MultipleResultsFound, NoResultFound
from couchdbkit.resource import CouchdbResource
from couchdbkit.ext.django import loading
from couchdbkit.ext.django.schema import DocumentMeta
from couchdbkit.client import Server, Database, ViewResults

COUCHDB_DATABASES = getattr(settings, "COUCHDB_DATABASES", [])
COUCHDB_TIMEOUT = getattr(settings, "COUCHDB_TIMEOUT", 300)

logger = logging.getLogger('couchdbkit.ext.django.paginate')

class CustomCouchdbkitHandler(loading.CouchdbkitHandler):
    def __init__(self, databases):
        """ initialize couchdbkit handler with COUCHDB_DATABASES
        settings """

        self.__dict__ = self.__shared_state__

        # Convert old style to new style
        if isinstance(databases, (list, tuple)):
            databases = dict(
                (app_name, {'URL': uri}) for app_name, uri in databases
            )

        # create databases sessions
        for app_name, app_setting in databases.iteritems():
            uri = app_setting['URL']

            # Blank credentials are valid for the admin party
            user = app_setting.get('USER', '')
            password = app_setting.get('PASSWORD', '')
            auth = BasicAuth(user, password)

            try:
                if isinstance(uri, (list, tuple)):
                    # case when you want to specify server uri
                    # and database name specifically. usefull
                    # when you proxy couchdb on some path
                    server_uri, dbname = uri
                else:
                    server_uri, dbname = uri.rsplit("/", 1)
            except ValueError:
                raise ValueError("couchdb uri [%s:%s] invalid" % (
                    app_name, uri))

            res = CouchdbResource(server_uri, timeout=COUCHDB_TIMEOUT, filters=[auth])

            server = self.create_server(server_uri, resource_instance=res)
            app_label = app_name.split('.')[-1]
            self._databases[app_label] = (server, dbname)
            
    def create_server(self, server_uri, resource_instance):
        return CustomServer(server_uri, resource_instance)
    
class CustomServer(Server):
    def get_db(self, dbname, **params):
        return CustomDatabase(self._db_uri(dbname), server=self, **params)

class CustomDatabase(Database):
    #https://github.com/nod/maroon/blob/master/couch.py
    def get_all(self, cls, limit=None):
        for doc in self.paged_view('_all_docs',include_docs=True,limit=limit):
            if doc['id'][0]!='_':
                yield cls(doc['doc'])
                
    def paged_view(self, view_name, page_size, cls=None, **params):
        if cls:
            params['include_docs']=True
            
        if not page_size:
            res = self.view(view_name, **params)
            for r in res:
                yield r
            return
        
        orig_limit = params.get('limit',None)
        yielded = 0
        params['limit']=page_size+1
        
        while True:
            if orig_limit is not None:
                params['limit']=min(orig_limit-yielded,page_size+1)
            
            view = self.view(view_name, **params)
            res = list(view)
                        
            logger.debug((" "*21)+("+"*119)+("> Database.fetch(rows=%s)"%(len(res[0:page_size]))))
            
            for r in res[0:page_size]:
                if cls:
                    yield cls(r['doc'])
                else:
                    yield r
            if len(res) != page_size+1:
                break
            yielded +=page_size
            last = res[-1]
            params['startkey']=last['_key']
            params['startkey_docid']=last._id  
            
    def view(self, view_name, schema=None, wrapper=None, **params):
        """ get view results from database. viewname is generally
        a string like 'designname/viewname". It return an ViewResults
        object on which you could iterate, list, ... . You could wrap
        results in wrapper function, a wrapper function take a row
        as argument. Wrapping could be also done by passing an Object
        in obj arguments. This Object should have a 'wrap' method
        that work like a simple wrapper function.

        @param view_name, string could be '_all_docs', '_all_docs_by_seq',
        'designname/viewname' if view_name start with a "/" it won't be parsed
        and beginning slash will be removed. Usefull with c-l for example.
        @param schema, Object with a wrapper function
        @param wrapper: function used to wrap results
        @param params: params of the view

        """

        if view_name.startswith('/'):
            view_name = view_name[1:]
        if view_name == '_all_docs':
            view_path = view_name
        elif view_name == '_all_docs_by_seq':
            view_path = view_name
        else:
            view_name = view_name.split('/')
            dname = view_name.pop(0)
            vname = '/'.join(view_name)
            view_path = '_design/%s/_view/%s' % (dname, vname)

        return CustomViewResults(self.raw_view, view_path, wrapper, schema, params)   
    
class CustomViewResults(ViewResults):
    def __init__(self, fetch, arg, wrapper, schema, params):        
        ViewResults.__init__(self, fetch, arg, wrapper, schema, params)
        
        #Overwrite wrapper        
        self.orig_wrapper = self.wrapper
        self.wrapper = self.extended_wrapper
        
    def extended_wrapper(self, row):        
        result = self.orig_wrapper(row)
        
        #If the result is a Schema, it is no longer a row. Standard CouchDBKit loses the "key" info
        #This function makes sure the key info is still available as a "private" attribute
        if isinstance(result, schema.Document):            
            result['_key'] = row['key'] 
        
        return result
   
class GeneratorViewResults(object):
    """
    Object to retrieve view results.
    """

    def __init__(self, generator):
        """
        Constructor of GeneratorViewResults object

        @param view: Object inherited from :mod:'couchdbkit.client.view.ViewInterface
        @param params: params to apply when fetching view.

        """
        self._generator = generator

    def iterator(self):
        return self._generator

    def first(self):
        """
        Return the first result of this query or None if the result doesn't contain any row.
        This results in an execution of the underlying query.
        """
        try:
            return list(self)[0]
        except IndexError:
            return None

    def one(self, except_all=False):
        """
        Return exactly one result or raise an exception.


        Raises 'couchdbkit.exceptions.MultipleResultsFound' if multiple rows are returned.
        If except_all is True, raises 'couchdbkit.exceptions.NoResultFound'
        if the query selects no rows.

        This results in an execution of the underlying query.
        """

        length = len(self)
        if length > 1:
            raise MultipleResultsFound("%s results found." % length)

        result = self.first()
        if result is None and except_all:
            raise NoResultFound
        return result

    def all(self):
        """ return list of all results """
        return list(self.iterator())

    def count(self):
        """ return number of returned results """
        return len(all())   

    @property
    def total_rows(self):
        """ return number of total rows in the view """
        # reduce case, count number of lines
        if self._total_rows is None:
            self._total_rows = self.count()
        return self._total_rows

    def __getitem__(self, key):
        return self.all().__getitem__(key)

    def __iter__(self):
        return self.iterator()

    def __len__(self):
        return self.count()

    def __nonzero__(self):
        return bool(len(self))
         
customCouchDbkitHandler = CustomCouchdbkitHandler(COUCHDB_DATABASES)
            
class CustomQueryMixin(object):
    """ Mixin that add query methods """
    
    @classmethod
    def paged_view(cls, view_name, wrapper=None, dynamic_properties=None,
    wrap_doc=True, classes=None, **params):
        """ Get documents associated view a view.
        Results of view are automatically wrapped
        to Document object.

        @params view_name: str, name of view
        @params wrapper: override default wrapper by your own
        @dynamic_properties: do we handle properties which aren't in
        the schema ? Default is True.
        @wrap_doc: If True, if a doc is present in the row it will be
        used for wrapping. Default is True.
        @params params:  params of view

        @return: :class:'simplecouchdb.core.ViewResults' instance. All
        results are wrapped to current document instance.
        """        
        page_size = params.get("page_size", None)
        
        if not page_size:
            #If no page_size is defined, use the standard view function
            return cls.view(view_name, wrapper=wrapper, 
                            dynamic_properties=dynamic_properties,
                            wrap_doc=wrap_doc, classes=classes, **params)
        
        db = cls.get_db()
        return GeneratorViewResults(
            db.paged_view(view_name, dynamic_properties=dynamic_properties, wrap_doc=wrap_doc,
                          wrapper=wrapper, schema=classes or cls, **params))
            
class Document(schema.Document, CustomQueryMixin):
    """ Document object for django extension """
    __metaclass__ = DocumentMeta

    get_id = property(lambda self: self['_id'])
    get_rev = property(lambda self: self['_rev'])

    @classmethod
    def get_db(cls):
        db = getattr(cls, '_db', None)
        if db is None:
            app_label = getattr(cls._meta, "app_label")
            db = customCouchDbkitHandler.get_db(app_label)
            cls._db = db
        return db