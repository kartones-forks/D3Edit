import sqlite3
dbfile = 'db/local.db'


AFFIXES_TABLE_FIELDS = "`affix_id`,`effect`,`effect_values`,`effect_simple`,`tier`"
ITEMS_TABLE_FIELDS = "`gbid`,`name`,`category`,`stackable`,`d3cat`"


class Database(object):

    """ DB initialization """
    def __init__(self, database=dbfile):
        self.cursor = None
        self.connection = None
        self.connected = False
        self.print = False
        self.database = database

    def __enter__(self):
        pass

    def connect(self):
        """Connects to DB"""
        self.connection = sqlite3.connect(self.database)
        self.cursor = self.connection.cursor()
        self.connected = True

    def close(self):
        self.connection.commit()
        self.connection.close()
        self.connected = False

    def execute(self, query=None):
        output = None
        if not query:
            return "No query specified"
        if not self.connected:
            self.connect()
        try:
            output = self.cursor.execute(query).fetchall()
        except sqlite3.Error as e:
            print("Error encountered: {}".format(e.args[0]))
            print("Running query: {}".format(query))
        if self.print:
            for row in output:
                print(row)
        else:
            return output

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def instance_and_run(query):
    dbo = Database()
    with dbo:
        out = dbo.execute(query)
    return out


def get_affix_from_id(affix_id):
    affix_id = int(affix_id)
    query = "SELECT {affixes_fields} FROM `affixes` WHERE `affix_id` = '{id}' ORDER BY `effect`".format(
        affixes_fields=AFFIXES_TABLE_FIELDS, id=affix_id
    )
    return instance_and_run(query)


def get_affix_from_effect(effect_simple):
    query = "SELECT {affixes_fields} FROM `affixes` WHERE `effect_simple` = '{effect_simple}' ORDER BY `effect`".format(
        affixes_fields=AFFIXES_TABLE_FIELDS, effect_simple=effect_simple
    )
    return instance_and_run(query)


def get_affix_all():
    query = "SELECT {affixes_fields} FROM `affixes` ORDER BY `effect`".format(affixes_fields=AFFIXES_TABLE_FIELDS)
    return instance_and_run(query)


def get_currency_list():
    query = "SELECT `currency_id`, `currency_name` FROM `currencies` ORDER BY `currency_id`"
    return instance_and_run(query)


def get_item_from_gbid(gbid):
    query = "SELECT {items_fields} FROM `items` where `gbid` = '{id}'".format(items_fields=ITEMS_TABLE_FIELDS, id=gbid)
    return instance_and_run(query)


def get_slot(slot_id):
    query = "SELECT `id`, `slot` FROM `slots` where `id` = '{id}'".format(id=slot_id)
    return instance_and_run(query)


def get_legal_affixes(category):
    query = "SELECT `affix_list` FROM `affix_groups` where `item_types` like '%{category}%'".format(category=category)
    return instance_and_run(query)


def get_quality_levels():
    query = "SELECT `level`, `quality` FROM `itemquality` ORDER BY `level`"
    return instance_and_run(query)


def get_quality_level(quality):
    query = "SELECT 1 FROM `itemquality` WHERE `quality` = '{quality}'".format(quality=quality)
    return instance_and_run(query)[0][0]


def get_categories():
    query = "SELECT DISTINCT(`category`) FROM `items` ORDER BY `category`"
    return instance_and_run(query)


def get_items_from_category(category):
    query = "SELECT `name`, `gbid` FROM `items` WHERE `category` = '{category}' ORDER BY `name`".format(
        category=category
    )
    return instance_and_run(query)
