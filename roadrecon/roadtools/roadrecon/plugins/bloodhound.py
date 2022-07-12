'''
Export ROADrecon data into BloodHound's neo4j database
Uses code from aclpwn under an MIT license

Copyright (c) 2020 Dirk-jan Mollema

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
'''
import platform
import os
import json
import argparse
import sys
from roadtools.roadlib.metadef.database import ServicePrincipal, User, Group, DirectoryRole, lnk_group_member_user
import roadtools.roadlib.metadef.database as database
try:
    from neo4j import GraphDatabase
    from neo4j.exceptions import ClientError
    HAS_NEO_MODULE = True
except ModuleNotFoundError:
    try:
        from neo4j.v1 import GraphDatabase
        from neo4j.exceptions import ClientError
        HAS_NEO_MODULE = True
    except ModuleNotFoundError:
        HAS_NEO_MODULE = False

DESCRIPTION = '''
Export ROADrecon data into BloodHound's neo4j database.
Requires a custom version of the BloodHound interface to use, available at
https://github.com/dirkjanm/BloodHound-AzureAD
'''

BASE_LINK_QUERY = 'UNWIND $props AS prop MERGE (n:{0} {{objectid: prop.source}}) MERGE (m:{1} {{objectid: prop.target}}) MERGE (n)-[r:{2}]->(m)';

def from_rel2edge(tx, atype, btype, linktype, db_iter, parent_id):
    #for r in db_iter:
    #    add_edge(tx, r.objectId, atype, parent_id, btype, linktype)
    col = []
    for r in db_iter:
        col.append({'source':r.objectId,  'target':parent_id})
        if len(col)>500:
            bulk_add_edges(tx, atype, btype, linktype, col)
            col = []
    if len(col)>0:
        bulk_add_edges(tx, atype, btype, linktype, col)

def bulk_add_edges(tx, atype, btype, linktype, props):
    q = BASE_LINK_QUERY.format(atype, btype, linktype)
    tx.run(q, props=props)

def add_edge(tx, aid, atype, bid, btype, linktype):
    q = BASE_LINK_QUERY.format(atype, btype, linktype)
    props = {'source':aid, 'target':bid}
    tx.run(q, props=props)

def _yield_limit(qry, pk_attr, maxrq=250):
    """specialized windowed query generator (using LIMIT/OFFSET)

    This recipe is to select through a large number of rows thats too
    large to fetch at once. The technique depends on the primary key
    of the FROM clause being an integer value, and selects items
    using LIMIT."""

    firstid = None
    while True:
        q = qry
        if firstid is not None:
            q = qry.filter(pk_attr > firstid)
        rec = None
        for rec in q.order_by(pk_attr).limit(maxrq):
            yield rec
        if rec is None:
            break
        firstid = pk_attr.__get__(rec, pk_attr) if rec else None

class BloodHoundPlugin():
    """
    Export data to BloodHounds neo4j database plugin
    """
    def __init__(self, session, dbhost, dbuser, dbpass):
        # SQLAlchemy session
        self.session = session
        # Neo4j driver
        self.driver = self.init_driver(dbhost, dbuser, dbpass)

    @staticmethod
    def init_driver(database, user, password):
        """
        Initialize neo4j driver
        """
        uri = "bolt://%s:7687" % database
        driver = GraphDatabase.driver(uri, auth=(user, password), encrypted=False)
        return driver

    @staticmethod
    def detect_db_config():
        """
        Detect bloodhound config, which is stored in appData.
        OS dependent according to https://electronjs.org/docs/api/app#appgetpathname
        """
        system = platform.system()
        if system == 'Windows':
            try:
                directory = os.environ['APPDATA']
            except KeyError:
                return (None, None)
            config = os.path.join(directory, 'BloodHound', 'config.json')
            try:
                with open(config, 'r') as configfile:
                    configdata = json.load(configfile)
            except IOError:
                return (None, None)

        if system == 'Linux':
            try:
                directory = os.environ['XDG_CONFIG_HOME']
            except KeyError:
                try:
                    directory = os.path.join(os.environ['HOME'], '.config')
                except KeyError:
                    return (None, None)
            config = os.path.join(directory, 'bloodhound', 'config.json')
            try:
                with open(config, 'r') as configfile:
                    configdata = json.load(configfile)
            except IOError:
                return (None, None)

        if system == 'Darwin':
            try:
                directory = os.path.join(os.environ['HOME'], 'Library', 'Application Support')
            except KeyError:
                return (None, None)
            config = os.path.join(directory, 'bloodhound', 'config.json')
            try:
                with open(config, 'r') as configfile:
                    configdata = json.load(configfile)
            except IOError:
                return (None, None)

        # If we are still here, we apparently found the config :)
        try:
            username = configdata['databaseInfo']['user']
        except KeyError:
            username = 'neo4j'
        try:
            password = configdata['databaseInfo']['password']
        except KeyError:
            password = None
        return username, password

    def main(self):
        """
        Main plugin logic. Simply connects to both databases and transforms the data
        """
        print('Connecting to neo4j')
        with self.driver.session() as neosession:
            print('Running queries')

            try:
                neosession.run('CREATE CONSTRAINT ON (c:AzureUser) ASSERT c.objectid IS UNIQUE')
                neosession.run('CREATE CONSTRAINT ON (c:AzureGroup) ASSERT c.objectid IS UNIQUE')
                neosession.run('CREATE CONSTRAINT ON (c:AzureRole) ASSERT c.objectid IS UNIQUE')
                neosession.run('CREATE CONSTRAINT ON (c:ServicePrincipal) ASSERT c.objectid IS UNIQUE')
            except ClientError as e:
                pass  # on neo4j 4, an error is raised when the constraint exists already

            all_done = self.session.query(User.objectId).count()
            done = 0
            for user in _yield_limit(self.session.query(User), User.objectId):
                property_query = 'UNWIND $props AS prop MERGE (n:AzureUser {objectid: prop.sourceid}) SET n += prop.map'
                uprops = {
                    'name': user.userPrincipalName,
                    'displayname': user.displayName,
                    'enabled': user.accountEnabled,
                    'distinguishedname': user.onPremisesDistinguishedName,
                    'email': user.mail,
                }
                props = {'map': uprops, 'sourceid': user.objectId}
                if user.onPremisesSecurityIdentifier:
                    # uprops['onPremisesSecurityIdentifier'] = user.onPremisesSecurityIdentifier
                    props['onpremid'] = user.onPremisesSecurityIdentifier
                    property_query = 'UNWIND $props AS prop MERGE (n:AzureUser {objectid: prop.sourceid}) MERGE (m:User {objectid:prop.onpremid}) MERGE (m)-[r:SyncsTo {isacl:false}]->(n) SET n += prop.map'
                res = neosession.run(property_query, props=props)
                p = (done*100)/all_done
                print(f"User {p:.3f}%                  \r", flush=True, end='')
                done += 1

            all_done = self.session.query(ServicePrincipal.objectId).count()
            done = 0
            for sprinc in _yield_limit(self.session.query(ServicePrincipal),ServicePrincipal.objectId):
                property_query = 'UNWIND $props AS prop MERGE (n:ServicePrincipal {objectid: prop.sourceid}) SET n += prop.map'
                uprops = {
                    'name': sprinc.displayName,
                    'appid': sprinc.appId,
                    'publisher': sprinc.publisherName,
                    'displayname': sprinc.displayName,
                    'enabled': sprinc.accountEnabled,
                }
                props = {'map': uprops, 'sourceid': sprinc.objectId}
                res = neosession.run(property_query, props=props)
                from_rel2edge(neosession, 'AzureUser', 'ServicePrincipal', 'Owns', sprinc.ownerUsers, sprinc.objectId)
                from_rel2edge(neosession, 'ServicePrincipal', 'ServicePrincipal', 'Owns', sprinc.ownerServicePrincipals, sprinc.objectId)
                p = (done*100)/all_done
                print(f"ServicePrincipal {p:.3f}%     \r", flush=True, end='')
                done += 1

            all_done = self.session.query(Group.objectId).count()
            done = 0
            for group in _yield_limit(self.session.query(Group), Group.objectId):
                property_query = 'UNWIND $props AS prop MERGE (n:AzureGroup {objectid: prop.sourceid}) SET n += prop.map'
                uprops = {
                    'name': group.displayName,
                    'displayname': group.displayName,
                    'email': group.mail,
                }
                props = {'map': uprops, 'sourceid': group.objectId}
                if group.onPremisesSecurityIdentifier:
                    # uprops['onPremisesSecurityIdentifier'] = group.onPremisesSecurityIdentifier
                    props['onpremid'] = group.onPremisesSecurityIdentifier
                    property_query = 'UNWIND $props AS prop MERGE (n:AzureGroup {objectid: prop.sourceid}) MERGE (m:Group {objectid:prop.onpremid}) MERGE (m)-[r:SyncsTo {isacl:false}]->(n) SET n += prop.map'

                res = neosession.run(property_query, props=props)
                #group.memberUsers can be huge!
                from_rel2edge(neosession, 'AzureUser', 'AzureGroup', 'MemberOf', _yield_limit(group.memberUsers, User.objectId), group.objectId)
                from_rel2edge(neosession, 'AzureGroup', 'AzureGroup', 'MemberOf', group.memberGroups, group.objectId)
                from_rel2edge(neosession, 'AzureGroup', 'ServicePrincipal', 'MemberOf', group.memberServicePrincipals, group.objectId)
                p = (done*100)/all_done
                print(f"Group {p:.3f}%               \r", flush=True, end='')
                done += 1


            all_done = self.session.query(DirectoryRole.objectId).count()
            done = 0
            for role in _yield_limit(self.session.query(DirectoryRole), DirectoryRole.objectId):
                property_query = 'UNWIND $props AS prop MERGE (n:AzureRole {objectid: prop.sourceid}) SET n += prop.map'
                uprops = {
                    'name': role.displayName,
                    'displayname': role.displayName,
                    'description': role.description,
                    'templateid': role.roleTemplateId
                }
                props = {'map': uprops, 'sourceid': role.objectId}
                res = neosession.run(property_query, props=props)

                from_rel2edge(neosession, 'AzureUser', 'AzureRole', 'MemberOf', role.memberUsers, role.objectId)
                from_rel2edge(neosession, 'ServicePrincipal', 'AzureRole', 'MemberOf', role.memberServicePrincipals, role.objectId)
                p = (done*100)/all_done
                print(f"DirectoryRole {p:.3f}%      \r", flush=True, end='')
                done += 1

        print('Done!           ')
        self.driver.close()

def add_args(parser):
    #DB parameters
    databasegroup = parser.add_argument_group("Database options (if unspecified they will be taken from your BloodHound config)")
    databasegroup.add_argument("--neodatabase", type=str, metavar="DATABASE HOST", default="localhost", help="The host neo4j is running on. Default: localhost")
    databasegroup.add_argument("-du", "--database-user", type=str, metavar="USERNAME", default="neo4j", help="Neo4j username to use")
    databasegroup.add_argument("-dp", "--database-password", type=str, metavar="PASSWORD", help="Neo4j password to use")


def main(args=None):
    if not HAS_NEO_MODULE:
        print('neo4j python module not found! Please install the module neo4j-driver first (pip install neo4j-driver)')
        sys.exit(1)
        return
    if args is None:
        parser = argparse.ArgumentParser(add_help=True, description='ROADrecon policies to HTML plugin', formatter_class=argparse.RawDescriptionHelpFormatter)
        parser.add_argument('-d',
                            '--database',
                            action='store',
                            help='Database file. Can be the local database name for SQLite, or an SQLAlchemy compatible URL such as postgresql+psycopg2://dirkjan@/roadtools',
                            default='roadrecon.db')
        add_args(parser)
        args = parser.parse_args()
    db_url = database.parse_db_argument(args.database)
    if args.database_password is None:
        args.database_user, args.database_password = BloodHoundPlugin.detect_db_config()
        if args.database_password is None:
            print('Error: Could not autodetect the Neo4j database credentials from your BloodHound config. Please specify them manually')
            return
    session = database.get_session(database.init(dburl=db_url))
    plugin = BloodHoundPlugin(session, args.neodatabase, args.database_user, args.database_password)
    plugin.main()

if __name__ == '__main__':
    main()
