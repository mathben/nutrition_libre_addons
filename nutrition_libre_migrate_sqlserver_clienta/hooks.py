import base64
import datetime
import re

# import locale
import logging
from collections import defaultdict

from odoo import SUPERUSER_ID, _, api, fields, models
from odoo.exceptions import ValidationError

_logger = logging.getLogger(__name__)

# TODO don't forget to increase memory
# --limit-memory-soft=8589934592 --limit-memory-hard=10737418240

HOST = "localhost"
USER = ""
PORT = "1433"
PASSWD = ""
DB_NAME = ""
BACKUP_PATH = "/tmp/"
FILE_PATH = f"{BACKUP_PATH}/document/doc"
DEBUG_OUTPUT = True
DEBUG_LIMIT = False
LIMIT = 20
DEFAULT_SELL_USER_ID = 2  # or 8
MIGRATE_SALE = True
MIGRATE_INVOICE = True
MIGRATE_COUPON = False
link_generic_video_demo = ""
USE_DISCOUNT_PERC = False
LST_KEY_EVENT = [""]
ENABLE_SELLER_MARKETPLACE = False

# try:
#     import pymssql
#
#     assert pymssql
# except ImportError:
#     raise ValidationError(
#         'pymssql is not available. Please install "pymssql" python package.'
#     )
# if not HOST or not USER or not PASSWD or not DB_NAME:
#     raise ValidationError(
#         f"Please, fill constant HOST/USER/PASSWD/DB_NAME into files {__file__}"
#     )


def post_init_hook(cr, e):
    env = api.Environment(cr, SUPERUSER_ID, {})
    _logger.info("Start migration")
    migration = Migration(cr)

    # General configuration
    migration.setup_configuration()

    # Migration method for each table
    migration.migrate_tbUsers()

    migration.migrate_tbStoreCategories()
    migration.migrate_tbStoreItems()
    for (
        item_id_i,
        product_id,
    ) in migration.dct_k_tbstoreitems_v_product_template.items():
        migration.migrate_tbStoreItemPictures(item_id_i, product_id)
    migration.migrate_tbTrainingCourses()
    for (
        obj_id_i,
        obj_slide_channel_id,
    ) in migration.dct_k_tbtrainingcourses_id_test_v_slide_channel.items():
        (
            obj_survey_id,
            first_knowledge_test_id,
        ) = migration.continue_migrate_tbTrainingCourses_knowledge_question(
            obj_id_i,
        )
        if obj_survey_id is False or first_knowledge_test_id is False:
            continue
        migration.continue_migrate_tbTrainingCourses_slide_slide(
            first_knowledge_test_id,
            obj_slide_channel_id,
            obj_survey_id,
        )

    migration.continue_migrate_tbTrainingCourses_knownledge_answer()

    # migration.migrate_tbStoreItemVariants()
    migration.migrate_tbCoupons()
    migration.migrate_tbStoreShoppingCarts()

    # Print warning
    if migration.lst_warning:
        print("Got warning :")
        lst_warning = list(set(migration.lst_warning))
        lst_warning.sort()
        for warn in lst_warning:
            print(f"\t{warn}")

    # Print error
    if migration.lst_error:
        print("Got error :")
        lst_error = list(set(migration.lst_error))
        lst_error.sort()
        for err in lst_error:
            print(f"\t{err}")

    # Print summary
    lst_model = [
        "res.partner",
        "res.users",
        "product.category",
        "slide.channel",
        "survey.survey",
        "survey.question",
        "survey.question.answer",
        "slide.slide",
        "survey.user_input",
        "survey.user_input.line",
        "slide.channel.partner",
        "slide.slide.partner",
        "event.event",
        "event.event.ticket",
        "product.template",
        "sale.order",
        "sale.order.line",
        "event.registration",
        "account.move",
        "account.payment",
        "loyalty.program",
        "loyalty.reward",
    ]
    print(f"Migrate into {len(lst_model)} models.")
    for model in lst_model:
        print(f"{len(env[model].search([]))} {model}")
    print("Statistic ignoring data migration")
    for key, value in migration.dct_data_skip.items():
        print(f"Table '{key}': {value}")


class Struct(object):
    def __init__(self, **entries):
        self.__dict__.update(entries)


class Migration:
    def __init__(self, cr):
        # Generic variable
        self.cr = cr
        self.lst_used_email = []
        self.lst_error = []
        self.lst_warning = []

        # Path of the backup
        self.source_code_path = BACKUP_PATH
        self.logo_path = f"{self.source_code_path}/images/logo"
        # Table into cache
        self.dct_tbanimators = {}
        self.dct_tbcontents = {}
        self.dct_tbcouponalloweditems = {}
        self.dct_k_tbcoupons_v_loyalty_program = {}
        self.dct_tbexpensecategories = {}
        self.dct_tbgalleryitems = {}
        self.dct_k_tbknowledgeanswerresults_v_survey_question_answer = {}
        self.dct_k_tbknowledgequestions_v_survey_question = {}
        self.dct_tbknowledgetestresults = {}
        self.dct_tbknowledgetests = {}
        self.dct_tbmailtemplates = {}
        self.dct_k_tbstorecategories_v_product_category = {}
        self.dct_tbstoreitemanimators = {}
        self.dct_tbstoreitemcontentpackagemappings = {}
        self.dct_tbstoreitemcontentpackages = {}
        self.dct_tbstoreitemcontents = {}
        self.dct_tbstoreitemcontenttypes = {}
        self.dct_k_tbstoreitempictures_v_product_template = {}
        self.dct_tbstoreitemtaxes = {}
        self.dct_tbstoreitemtrainingcourses = {}
        self.dct_tbstoreitemvariants = {}
        self.dct_tbstoreshoppingcartitemcoupons = {}
        self.dct_tbstoreshoppingcartitems = {}
        self.dct_k_tbstoreshoppingcarts_v_sale_order = {}
        self.dct_k_tbtrainingcourses_id_test_v_slide_channel = {}
        self.dct_k_tbtrainingcourses_v_slide_channel = {}
        self.dct_tbusers = {}
        self.dct_data_skip = defaultdict(int)
        self.dct_k_formation_name_v_product_template = defaultdict(list)
        self.dct_k_formation_name_v_slide_channel = {}
        # Model into cache
        self.dct_res_user_id = {}
        self.dct_partner_id = {}
        self.dct_k_knowledgetest_v_survey_id = {}
        self.dct_k_survey_v_slide_survey_id = {}
        # self.dct_k_tbstoreitems_v_event_ticket = {}
        self.dct_k_tbstoreitems_v_product_template = {}
        # self.dct_k_tbstoreitems_v_event_event = {}
        # local variable
        self.sale_tax_id = None
        self.sale_tax_TPS_id = None
        self.sale_tax_TVQ_id = None
        self.purchase_tax_id = None
        self.default_product_frais_id = None
        # Database information
        import pymssql

        self.host = HOST
        self.user = USER
        self.port = PORT
        self.passwd = PASSWD
        self.db_name = DB_NAME
        self.conn = pymssql.connect(
            server=self.host,
            user=self.user,
            port=self.port,
            password=self.passwd,
            database=self.db_name,
            # charset="utf8",
            # use_unicode=True,
        )
        self.dct_tbl = self._fill_tbl()

    def _fill_tbl(self):
        """
        Fill all database in self.dct_tbl
        :return:
        """
        cur = self.conn.cursor()
        # Get all tables
        str_query = (
            f"""SELECT * FROM {self.db_name}.INFORMATION_SCHEMA.TABLES;"""
        )
        cur.nextset()
        cur.execute(str_query)
        tpl_result = cur.fetchall()

        lst_whitelist_table = [
            "tbAnimators",
            "tbContents",
            "tbCouponAllowedItems",
            "tbCoupons",
            "tbExpenseCategories",
            "tbGalleryItems",
            "tbKnowledgeAnswerChoices",
            "tbKnowledgeAnswerResults",
            "tbKnowledgeQuestions",
            "tbKnowledgeTestResults",
            "tbKnowledgeTests",
            "tbMailTemplates",
            "tbStoreCategories",
            "tbStoreItemAnimators",
            "tbStoreItemContentPackageMappings",
            "tbStoreItemContentPackages",
            "tbStoreItemContents",
            "tbStoreItemContentTypes",
            "tbStoreItemPictures",
            "tbStoreItems",
            "tbStoreItemTaxes",
            "tbStoreItemTrainingCourses",
            "tbStoreItemVariants",
            "tbStoreShoppingCartItemCoupons",
            "tbStoreShoppingCartItems",
            "tbStoreShoppingCartItemTaxes",
            "tbStoreShoppingCarts",
            "tbTrainingCourses",
            "tbUsers",
        ]

        dct_tbl = {f"{a[0]}.{a[1]}.{a[2]}": [] for a in tpl_result}
        dct_short_tbl = {f"{a[0]}.{a[1]}.{a[2]}": a[2] for a in tpl_result}

        for table_name, lst_column in dct_tbl.items():
            table = dct_short_tbl[table_name]
            if table not in lst_whitelist_table:
                # msg = f"Skip table '{table}'"
                # _logger.warning(msg)
                # self.lst_warning.append(msg)
                continue

            _logger.info(f"Import in cache table '{table}'")
            str_query = f"""SELECT COLUMN_NAME FROM {self.db_name}.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = N'{table}';"""
            cur.nextset()
            cur.execute(str_query)
            tpl_result = cur.fetchall()
            lst_column_name = [a[0] for a in tpl_result]
            if table == "tbStoreShoppingCarts":
                # str_query = f"""SELECT * FROM {table_name} WHERE IsCompleted = 1 or ProviderStatusText = 'completed';"""
                str_query = f"""SELECT * FROM {table_name} WHERE ProviderStatusText = 'completed' ORDER BY CartID;"""
            else:
                str_query = f"""SELECT * FROM {table_name};"""
            cur.nextset()
            cur.execute(str_query)
            tpl_result = cur.fetchall()

            for lst_result in tpl_result:
                i = -1
                dct_value = {}
                for result in lst_result:
                    i += 1
                    dct_value[lst_column_name[i]] = result
                lst_column.append(Struct(**dct_value))

        return dct_tbl

    def setup_configuration(self, dry_run=False):
        _logger.info("Setup configuration")

        env = api.Environment(self.cr, SUPERUSER_ID, {})
        # General configuration
        values = {
            # "group_product_variant": True,
            "group_discount_per_so_line": True,
        }
        if not dry_run:
            event_config = env["res.config.settings"].sudo().create(values)
            event_config.execute()
        companies = env["res.company"].search([])
        for company in companies:
            # TPS + TVQ ventes
            sale_tax_id = env["account.tax"].search(
                [
                    ("company_id", "=", company.id),
                    ("name", "=", "TPS + TVQ sur les ventes"),
                ]
            )
            self.sale_tax_id = sale_tax_id
            company.account_sale_tax_id = sale_tax_id.id

            # TPS ventes
            sale_tax_TPS_id = env["account.tax"].search(
                [
                    ("company_id", "=", company.id),
                    ("name", "=", "TPS sur les ventes - 5%"),
                ]
            )
            self.sale_tax_TPS_id = sale_tax_TPS_id

            # TVQ ventes
            sale_tax_TVQ_id = env["account.tax"].search(
                [
                    ("company_id", "=", company.id),
                    ("name", "=", "TVQ sur les ventes - 9,975%"),
                ]
            )
            self.sale_tax_TVQ_id = sale_tax_TVQ_id

            # TPS + TVQ achat
            purchase_tax_id = env["account.tax"].search(
                [
                    ("company_id", "=", company.id),
                    ("name", "=", "TPS + TVQ sur les achats"),
                ]
            )
            self.purchase_tax_id = purchase_tax_id
            company.account_purchase_tax_id = purchase_tax_id.id
            # Configure journal for cash
            journal_id = env["account.journal"].search(
                domain=[("type", "=", "cash")],
                limit=1,
            )
            # Configure
            journal_id.inbound_payment_method_line_ids[
                0
            ].payment_account_id = journal_id.default_account_id.id
            journal_id.outbound_payment_method_line_ids[
                0
            ].payment_account_id = journal_id.default_account_id.id
            # Configure tax event
            env.ref("website_sale_slides.default_product_course").taxes_id = [
                (6, 0, self.sale_tax_id.ids)
            ]

    def migrate_tbCoupons(self):
        """
        :return:
        """
        if not MIGRATE_COUPON:
            return
        _logger.info("Migrate tbCoupons")
        env = api.Environment(self.cr, SUPERUSER_ID, {})
        if self.dct_k_tbcoupons_v_loyalty_program:
            return
        table_name = f"{self.db_name}.dbo.tbCoupons"
        lst_tbl_tbcoupons = self.dct_tbl.get(table_name)
        lst_tbl_tbCouponAllowedItems = self.dct_tbl.get(
            f"{self.db_name}.dbo.tbCouponAllowedItems"
        )
        model_name = "loyalty.program"

        for i, tbcoupons in enumerate(lst_tbl_tbcoupons):
            if DEBUG_LIMIT and i > LIMIT:
                self.dct_data_skip[table_name] += len(lst_tbl_tbcoupons) - i
                break

            pos_id = f"{i+1}/{len(lst_tbl_tbcoupons)}"
            obj_id_i = tbcoupons.CouponID
            name = tbcoupons.CouponCode

            lst_associate_item = [
                a
                for a in lst_tbl_tbCouponAllowedItems
                if a.CouponID == obj_id_i
            ]

            if not lst_associate_item:
                continue

            value = {
                "name": name,
                "active": tbcoupons.IsActive,
            }

            obj_coupon_id = env[model_name].create(value)

            self.dct_k_tbcoupons_v_loyalty_program[obj_id_i] = obj_coupon_id
            if DEBUG_OUTPUT:
                _logger.info(
                    f"{pos_id} - {model_name} - table {table_name} - ADDED"
                    f" '{name}' id {obj_id_i}"
                )

            if tbcoupons.CouponAmount > 0.0:
                value_reward = {
                    "active": tbcoupons.IsActive,
                    "discount": tbcoupons.CouponAmount * 100,
                    "program_id": obj_coupon_id.id,
                    "discount_mode": (
                        "percent" if tbcoupons.IsPercent else "per_order"
                    ),
                }
                obj_coupon_reward_id = env["loyalty.reward"].create(
                    value_reward
                )
            lst_product = []
            for associate_item in lst_associate_item:
                product_id = self.dct_k_tbstoreitems_v_product_template.get(
                    associate_item.StoreItemID
                )
                if product_id:
                    lst_product.append(product_id.id)
                else:
                    msg = f"Missing product id {associate_item.StoreItemID}"
                    _logger.warning(msg)
                    self.lst_warning.append(msg)

            if lst_associate_item and not lst_product:
                msg = (
                    f"Coupon id {obj_id_i} has not product associate, it's"
                    " empty."
                )
                _logger.warning(msg)
                self.lst_warning.append(msg)

            if lst_product or tbcoupons.MinimumAmount > 0.0:
                value_rule = {
                    "active": tbcoupons.IsActive,
                    "program_id": obj_coupon_id.id,
                    "product_ids": [(6, 0, lst_product)],
                }
                if tbcoupons.MinimumAmount:
                    value_rule["minimum_amount"] = tbcoupons.MinimumAmount
                obj_coupon_rule_id = env["loyalty.rule"].create(value_rule)

    def migrate_tbStoreCategories(self):
        """
        :return:
        """
        _logger.info("Migrate tbStoreCategories")
        env = api.Environment(self.cr, SUPERUSER_ID, {})
        if self.dct_k_tbstorecategories_v_product_category:
            return
        table_name = f"{self.db_name}.dbo.tbStoreCategories"
        lst_tbl_tbstorecategories = self.dct_tbl.get(table_name)
        model_name = "product.category"

        for i, tbstorecategories in enumerate(lst_tbl_tbstorecategories):
            if DEBUG_LIMIT and i > LIMIT:
                self.dct_data_skip[table_name] += (
                    len(lst_tbl_tbstorecategories) - i
                )
                break

            pos_id = f"{i+1}/{len(lst_tbl_tbstorecategories)}"

            # TODO AffiliateLinks
            obj_id_i = tbstorecategories.CategoryID
            name = tbstorecategories.CategoryNameFR

            value = {
                "name": name,
            }

            obj_product_category_id = env[model_name].create(value)

            self.dct_k_tbstorecategories_v_product_category[obj_id_i] = (
                obj_product_category_id
            )
            if DEBUG_OUTPUT:
                _logger.info(
                    f"{pos_id} - {model_name} - table {table_name} - ADDED"
                    f" '{name}' id {obj_id_i}"
                )

    def migrate_tbStoreItems(self):
        """
        :return:
        """
        _logger.info("Migrate tbStoreItems")
        if self.dct_k_tbstoreitems_v_product_template:
            return
        env = api.Environment(self.cr, SUPERUSER_ID, {})
        default_user_seller_id = self.dct_res_user_id[DEFAULT_SELL_USER_ID]
        default_seller_id = self.dct_partner_id[DEFAULT_SELL_USER_ID]
        table_name = f"{self.db_name}.dbo.tbStoreItems"
        lst_tbl_tbstoreitems = self.dct_tbl.get(table_name)
        lst_tbl_tbstoreitemTaxes = self.dct_tbl.get(
            f"{self.db_name}.dbo.tbStoreItemTaxes"
        )
        model_name = "product.template"
        pattern_formation_no = r"\b\d+\.\w+\b"

        value_product = {
            "name": "Frais inconnu",
            "list_price": 1,
            "standard_price": 0,
            "description_sale": "Frais non déterminé.",
            "taxes_id": [(6, 0, self.sale_tax_id.ids)],
            "detailed_type": "service",
        }
        self.default_product_frais_id = env[model_name].create(value_product)

        for i, tbstoreitems in enumerate(lst_tbl_tbstoreitems):
            if DEBUG_LIMIT and i > LIMIT:
                self.dct_data_skip[table_name] += len(lst_tbl_tbstoreitems) - i
                break

            pos_id = f"{i+1}/{len(lst_tbl_tbstoreitems)}"
            obj_id_i = tbstoreitems.ItemID
            # ? ItemOrder
            # ? ItemShippingFee
            # DateCreated
            # ItemSellPrice
            # ItemBuyCost
            # ItemDescriptionFR
            # ItemDescriptionExtentedFR
            # if tbstoreitems.CategoryID in (1, 2):
            #     date_begin = tbstoreitems.DateCreated
            #     # if "DATE" in tbstoreitems.ItemDescriptionExtendedFR:
            #     #     pos_date = tbstoreitems.ItemDescriptionExtendedFR.index(
            #     #         "DATE :"
            #     #     )
            #     #     pos_end_date = (
            #     #         tbstoreitems.ItemDescriptionExtendedFR.index(
            #     #             "<br", pos_date
            #     #         )
            #     #     )
            #     #     extract_date = tbstoreitems.ItemDescriptionExtendedFR[
            #     #         pos_date + 6 : pos_end_date
            #     #     ].strip()
            #     #     if extract_date:
            #     #         locale.setlocale(locale.LC_ALL, "fr_CA")
            #     #         date_begin = datetime.datetime.strptime(
            #     #             extract_date, "%d %B %Y"
            #     #         )
            #     value_event = {
            #         "name": tbstoreitems.ItemNameFR,
            #         "user_id": default_user_seller_id.id,
            #         "organizer_id": default_seller_id.id,
            #         "create_date": tbstoreitems.DateCreated,
            #         "date_begin": date_begin,
            #         "date_end": date_begin,
            #         "date_tz": "America/Montreal",
            #         "is_published": tbstoreitems.IsOnHomePage,
            #         "active": tbstoreitems.IsActive,
            #     }
            #     event_id = env[model_name].create(value_event)
            #     self.dct_k_tbstoreitems_v_event_event[obj_id_i] = event_id
            #     price = tbstoreitems.ItemSellPrice
            #     value_event_ticket = {
            #         "name": tbstoreitems.ItemNameFR,
            #         "event_id": event_id.id,
            #         "product_id": env.ref(
            #             "event_sale.product_product_event"
            #         ).id,
            #         "price": price,
            #         "create_date": tbstoreitems.DateCreated,
            #     }
            #     event_ticket_id = env["event.event.ticket"].create(
            #         value_event_ticket
            #     )
            #     self.dct_k_tbstoreitems_v_event_ticket[
            #         obj_id_i
            #     ] = event_ticket_id
            # else:
            categorie_id = self.dct_k_tbstorecategories_v_product_category.get(
                tbstoreitems.CategoryID
            )
            # Search taxes
            taxes_item = [
                a.TaxID
                for a in lst_tbl_tbstoreitemTaxes
                if a.ItemID == obj_id_i
            ]
            taxes_item_ids = []
            if 1 in taxes_item and 2 in taxes_item:
                taxes_item_ids = self.sale_tax_id.ids
            elif 1 in taxes_item:
                taxes_item_ids = self.sale_tax_TPS_id.ids
            elif 2 in taxes_item:
                taxes_item_ids = self.sale_tax_TVQ_id.ids

            website_description = f"""<section class="s_text_block pt40 pb40 o_colored_level" data-snippet="s_text_block" data-name="Texte" style="background-image: none;">
<div class="container s_allow_columns">
    <p class="o_default_snippet_text">{tbstoreitems.ItemDescriptionExtendedFR}</p>
</div>
</section>"""
            value_product = {
                "name": tbstoreitems.ItemNameFR,
                "list_price": tbstoreitems.ItemSellPrice,
                "standard_price": tbstoreitems.ItemBuyCost,
                "create_date": tbstoreitems.DateCreated,
                "categ_id": categorie_id.id,
                "is_published": tbstoreitems.IsOnHomePage,
                "active": tbstoreitems.IsActive,
                "description_sale": tbstoreitems.ItemDescriptionFR,
                "taxes_id": [(6, 0, taxes_item_ids)],
                "purchase_ok": False,
            }
            ignore_creation = False
            product_template_id = None
            no_formation = None
            if tbstoreitems.CategoryID in (1, 2):
                value_product["detailed_type"] = "course"
                # Create product_template_id
                match = re.search(
                    pattern_formation_no, tbstoreitems.ItemNameFR
                )
                if match:
                    no_formation = match.group()
                else:
                    msg = f"Cannot find no_formation into item {tbstoreitems.ItemNameFR}"
                    _logger.warning(msg)
                    self.lst_warning.append(msg)
                    continue
                if (
                    no_formation
                    in self.dct_k_formation_name_v_product_template.keys()
                ):
                    ignore_creation = True
                    product_template_id = (
                        self.dct_k_formation_name_v_product_template[
                            no_formation
                        ][0]
                    )
                    # Migration message
                    print("")
                    # TODO mettre ancien prix et ancien nom
                    comment_message = (
                        "Merge avec "
                        f" #{tbstoreitems.ItemID} {tbstoreitems.ItemNameFR}<br/>Date"
                        f" {tbstoreitems.DateCreated}.<br/>Prix de vente {tbstoreitems.ItemSellPrice}$"
                    )
                    comment_value = {
                        "subject": (
                            "Note de migration - Plateforme ASP.net avant migration -"
                            f" Item No. {tbstoreitems.ItemID} a été mergé."
                        ),
                        "body": f"<p>{comment_message}</p>",
                        "parent_id": False,
                        "message_type": "comment",
                        "author_id": SUPERUSER_ID,
                        "model": model_name,
                        "res_id": product_template_id.id,
                    }
                    env["mail.message"].create(comment_value)

            if tbstoreitems.ItemDescriptionExtendedFR:
                value_product["website_description"] = website_description
            if not ignore_creation:
                product_template_id = env[model_name].create(value_product)
                if no_formation:
                    self.dct_k_formation_name_v_product_template[
                        no_formation
                    ].append(product_template_id)
                # Migration message
                comment_message = (
                    "Migration item "
                    f" #{tbstoreitems.ItemID} {tbstoreitems.ItemNameFR}<br/>Date"
                    f" {tbstoreitems.DateCreated}"
                )
                comment_value = {
                    "subject": (
                        "Note de migration - Plateforme ASP.net avant migration -"
                        f" Item No. {tbstoreitems.ItemID}"
                    ),
                    "body": f"<p>{comment_message}</p>",
                    "parent_id": False,
                    "message_type": "comment",
                    "author_id": SUPERUSER_ID,
                    "model": model_name,
                    "res_id": product_template_id.id,
                }
                env["mail.message"].create(comment_value)

            if product_template_id:
                self.dct_k_tbstoreitems_v_product_template[obj_id_i] = (
                    product_template_id
                )

            if DEBUG_OUTPUT:
                _logger.info(
                    f"{pos_id} - {model_name} - table {table_name} - ADDED"
                    f" '{tbstoreitems.ItemNameFR}' id {obj_id_i}"
                )

    def migrate_tbStoreItemPictures(self, item_id_i, product_id):
        """
        :return:
        """
        _logger.info("Migrate tbStoreItemPictures")
        env = api.Environment(self.cr, SUPERUSER_ID, {})
        table_name = f"{self.db_name}.dbo.tbStoreItemPictures"
        lst_tbl_tbstoreitempictures = self.dct_tbl.get(table_name)
        model_name = "product.template"

        lst_tbl_tbstoreitempictures_filter = [
            a for a in lst_tbl_tbstoreitempictures if a.ItemID == item_id_i
        ]

        for i, tbstoreitempictures in enumerate(
            lst_tbl_tbstoreitempictures_filter
        ):
            if DEBUG_LIMIT and i > LIMIT:
                self.dct_data_skip[table_name] += (
                    len(lst_tbl_tbstoreitempictures_filter) - i
                )
                break

            pos_id = f"{i+1}/{len(lst_tbl_tbstoreitempictures_filter)}"
            b64_image = base64.b64encode(tbstoreitempictures.Image)
            if i == len(lst_tbl_tbstoreitempictures_filter) - 1:
                # Force take last image
                product_id.image_1920 = b64_image
            value_image = {
                "name": f"{product_id.name}_{i}",
                "image_1920": b64_image,
                "product_tmpl_id": product_id.id,
            }
            env["product.image"].create(value_image)
            obj_id_i = tbstoreitempictures.PictureID
            self.dct_k_tbstoreitempictures_v_product_template[obj_id_i] = (
                product_id
            )
            if DEBUG_OUTPUT:
                _logger.info(
                    f"{pos_id} - {model_name} - table {table_name} - ADDED"
                    f" '{product_id.name}' id {obj_id_i}"
                )

    def migrate_tbStoreItemVariants(self):
        """
        :return:
        """
        _logger.info("Migrate tbStoreItemVariants")
        env = api.Environment(self.cr, SUPERUSER_ID, {})
        if self.dct_tbstoreitemvariants:
            return
        table_name = f"{self.db_name}.dbo.tbStoreItemVariants"
        lst_tbl_tbstoreitemvariants = self.dct_tbl.get(table_name)
        model_name = "product.attribute.value"

        # Create generic product attribute
        value_product_attribute = {"name": "HarmonieSanté avant migration"}
        product_attribute_id = env["product.attribute"].create(
            value_product_attribute
        )

        for i, tbstoreitemvariants in enumerate(lst_tbl_tbstoreitemvariants):
            if DEBUG_LIMIT and i > LIMIT:
                self.dct_data_skip[table_name] += (
                    len(lst_tbl_tbstoreitemvariants) - i
                )
                break

            # TODO not finish to implement, no need it

            pos_id = f"{i+1}/{len(lst_tbl_tbstoreitemvariants)}"
            # ItemID
            # VariantID
            # VariantOrder - sequence
            # VariantSellPrice diff avec prix d'avant
            obj_id_i = tbstoreitemvariants.VariantID
            name = tbstoreitemvariants.VariantNameFR

            value = {
                "name": name,
                "attribute_id": product_attribute_id.id,
            }

            # Check if exist before, do unique
            obj_id = env[model_name].search([("name", "=", name)])
            if not obj_id:
                obj_id = env[model_name].create(value)

            product_template_id = (
                self.dct_k_tbstoreitems_v_product_template.get(
                    tbstoreitemvariants.ItemID
                )
            )

            if not product_template_id:
                msg = (
                    "Cannot find product for ItemID"
                    f" '{tbstoreitemvariants.ItemID}'"
                )
                _logger.warning(msg)
                self.lst_warning.append(msg)
                self.dct_data_skip[table_name] += 1
                continue

            value_product_template_attribute_line = {
                "active": tbstoreitemvariants.IsActive,
                "product_tmpl_id": product_template_id.id,
            }

            env["product.template.attribute.line"].create(
                value_product_template_attribute_line
            )

            self.dct_tbstoreitemvariants[obj_id_i] = obj_id
            _logger.info(
                f"{pos_id} - {model_name} - table {table_name} - ADDED"
                f" '{name}' id {obj_id_i}"
            )

    def migrate_tbStoreShoppingCarts(self):
        """
        :return:
        """
        if not MIGRATE_SALE:
            return
        _logger.info("Migrate tbStoreShoppingCarts")
        env = api.Environment(self.cr, SUPERUSER_ID, {})
        if self.dct_k_tbstoreshoppingcarts_v_sale_order:
            return
        table_name = f"{self.db_name}.dbo.tbStoreShoppingCarts"
        lst_tbl_tbstoreshoppingcarts = self.dct_tbl.get(table_name)
        table_name = f"{self.db_name}.dbo.tbStoreShoppingCartItemCoupons"
        lst_tbl_tbstoreshoppingcartitemcoupons = self.dct_tbl.get(table_name)
        lst_tbl_tbstoreshoppingcartitemtaxes = self.dct_tbl.get(
            f"{self.db_name}.dbo.tbStoreShoppingCartItemTaxes"
        )
        lst_tbl_store_shopping_cart_item = self.dct_tbl.get(
            f"{self.db_name}.dbo.tbStoreShoppingCartItems"
        )
        default_account_client_recv_id = env["account.account"].search(
            domain=[("account_type", "=", "asset_receivable")], limit=1
        )
        # Configure journal for cash
        journal_id = env["account.journal"].search(
            domain=[("type", "=", "cash")],
            limit=1,
        )
        journal_sale_id = env["account.journal"].search(
            [("type", "=", "sale"), ("company_id", "=", env.company.id)]
        )[0]
        model_name = "sale.order"

        # Transform taxes for faster calcul
        dct_taxes_cart_item_id = defaultdict(list)
        for taxe_item in lst_tbl_tbstoreshoppingcartitemtaxes:
            dct_taxes_cart_item_id[taxe_item.CartItemID].append(taxe_item)

        for i, tbstoreshoppingcarts in enumerate(lst_tbl_tbstoreshoppingcarts):
            if DEBUG_LIMIT and i > LIMIT:
                self.dct_data_skip[table_name] += (
                    len(lst_tbl_tbstoreshoppingcarts) - i
                )
                break

            pos_id = f"{i+1}/{len(lst_tbl_tbstoreshoppingcarts)}"
            obj_id_i = tbstoreshoppingcarts.CartID
            date_created = tbstoreshoppingcarts.DateCreated
            date_paid = date_created
            if tbstoreshoppingcarts.DatePaid:
                date_paid = tbstoreshoppingcarts.DatePaid
            if tbstoreshoppingcarts.OrderDate:
                date_order = tbstoreshoppingcarts.OrderDate
            else:
                date_order = date_paid

            # if (
            #     not tbstoreshoppingcarts.IsCompleted
            #     and tbstoreshoppingcarts.ProviderStatusText != "completed"
            # ):
            #     continue
            if tbstoreshoppingcarts.ProviderStatusText != "completed":
                continue
            order_partner_id = self.dct_partner_id.get(
                tbstoreshoppingcarts.UserID
            )
            if not order_partner_id:
                # Will force public partner
                order_partner_id = env.ref("base.public_partner")
                # _logger.error(
                #     f"Cannot find client {store_shopping_cart.UserID} into"
                #     f" order {store_shopping_cart.CartID}"
                # )
                # continue
            # TODO check store_shopping_cart.ProviderStatusText
            # TODO check store_shopping_cart.ProviderTransactionID
            # TODO check store_shopping_cart.TotalAmount
            # TODO check store_shopping_cart.TotalDiscount
            value_sale_order = {
                # "name": store_shopping_cart.ItemNameFR,
                # "list_price": store_item.ItemSellPrice,
                # "standard_price": store_item.ItemBuyCost,
                "date_order": date_order,
                "create_date": date_order,
                "partner_id": order_partner_id.id,
                # "is_published": store_item.IsActive,
                "state": "sale",
                "client_order_ref": str(tbstoreshoppingcarts.CartID),
                "note": '<p>Conditions générales : <a href="https://harmoniesante.com/terms" target="_blank" rel="noreferrer noopener">https://harmoniesante.com/terms</a> </p>',
                "message_partner_ids": [(6, 0, [order_partner_id.id])],
            }
            sale_order_id = env[model_name].create(value_sale_order)
            # move.action_post()
            self.dct_k_tbstoreshoppingcarts_v_sale_order[
                tbstoreshoppingcarts.CartID
            ] = sale_order_id
            lst_items = [
                a
                for a in lst_tbl_store_shopping_cart_item
                if a.CartID == tbstoreshoppingcarts.CartID
            ]
            # Migration message
            comment_message = (
                "Transaction"
                f" #{tbstoreshoppingcarts.ProviderTransactionID} {tbstoreshoppingcarts.ProviderStatusText}<br/>Date"
                f" {date_created}<br/>Date order {date_order}<br/>Date paid"
                f" {date_paid}<br/>Total amount"
                f" {tbstoreshoppingcarts.TotalAmount}. Total discount"
                f" {tbstoreshoppingcarts.TotalDiscount}"
            )
            comment_value = {
                "subject": (
                    "Note de migration - Plateforme ASP.net avant migration -"
                    f" Commande No. {tbstoreshoppingcarts.CartID}"
                ),
                "body": f"<p>{comment_message}</p>",
                "parent_id": False,
                "message_type": "comment",
                "author_id": SUPERUSER_ID,
                "model": "sale.order",
                "res_id": sale_order_id.id,
            }
            env["mail.message"].create(comment_value)
            if not lst_items:
                # Create a new one
                value_sale_order_line = {
                    "name": "Non défini",
                    # "list_price": store_item.ItemSellPrice,
                    # "standard_price": store_item.ItemBuyCost,
                    "create_date": date_order,
                    "order_partner_id": order_partner_id.id,
                    "order_id": sale_order_id.id,
                    "price_unit": tbstoreshoppingcarts.TotalAmount / 1.14975,
                    "product_uom_qty": 1,
                    "display_type": False,
                    "product_id": self.default_product_frais_id.id,
                    # "tax_ids":
                    # "is_published": store_item.IsActive,
                }
                sale_order_line_id = env["sale.order.line"].create(
                    value_sale_order_line
                )
                _logger.error(
                    "Need more information, missing charts items for chart"
                    f" {tbstoreshoppingcarts.CartID}. Date"
                    f" {date_order}"
                )
            else:
                for item in lst_items:
                    product_shopping_id = (
                        self.dct_k_tbstoreitems_v_product_template.get(
                            item.ItemID
                        )
                    )
                    # event_shopping_id = None
                    # event_registration_id = None
                    if not product_shopping_id:
                        # event_shopping_id = (
                        #     self.dct_k_tbstoreitems_v_event_event.get(
                        #         item.ItemID
                        #     )
                        # )
                        # event_ticket_shopping_id = (
                        #     self.dct_k_tbstoreitems_v_event_ticket.get(
                        #         item.ItemID
                        #     )
                        # )
                        # product_shopping_id = env.ref(
                        #     "event_sale.product_product_event"
                        # )
                        # if not event_shopping_id:
                        #     _logger.error(
                        #         f"Cannot find product id {item.ItemID}"
                        #     )
                        #     # self.dct_data_skip[lst_tbl_knowledge_answer_results] += 1
                        #     continue
                        # name = "ticket"
                        #
                        # value_event_registration = {
                        #     "event_id": event_shopping_id.id,
                        #     "event_ticket_id": event_ticket_shopping_id.id,
                        #     "partner_id": order_partner_id.id,
                        # }
                        # # TODO state change open to done when event is done
                        # event_registration_id = env[
                        #     "event.registration"
                        # ].create(value_event_registration)
                        raise Exception("error event disable")
                    else:
                        name = product_shopping_id.name
                    value_sale_order_line = {
                        "name": name,
                        # "list_price": store_item.ItemSellPrice,
                        # "standard_price": store_item.ItemBuyCost,
                        "create_date": date_order,
                        "order_partner_id": order_partner_id.id,
                        "order_id": sale_order_id.id,
                        "price_unit": item.ItemSellPrice,
                        "product_uom_qty": item.Quantity,
                        "product_id": product_shopping_id.id,
                        # "is_published": store_item.IsActive,
                    }
                    # Search if missing taxes
                    lst_taxes = dct_taxes_cart_item_id.get(item.CartItemID)
                    if not lst_taxes:
                        # Force to remove taxes
                        value_sale_order_line["tax_id"] = [(6, 0, [])]
                    pre_calculated_sell_price = (
                        item.ItemSellPrice * item.Quantity
                    )
                    discount_price = 0.0
                    if (
                        item.ItemCalculatedSellPrice
                        != pre_calculated_sell_price
                        and item.ItemCalculatedSellPrice is not None
                    ):
                        discount_price = (
                            pre_calculated_sell_price
                            - item.ItemCalculatedSellPrice
                        )
                        if discount_price < 0:
                            msg = (
                                f"Cart {tbstoreshoppingcarts.CartID} for item id"
                                f" {item.ItemID}, discount is negative: {discount_price}. Date"
                                f" {date_paid}"
                            )
                            _logger.warning(msg)
                            self.lst_warning.append(msg)
                        if USE_DISCOUNT_PERC:
                            value_sale_order_line["discount"] = (
                                discount_price
                                / pre_calculated_sell_price
                                * 100.0
                            )
                        else:
                            value_sale_order_line["discount_fixed"] = (
                                discount_price
                            )
                    elif tbstoreshoppingcarts.TotalDiscount:
                        discount_price = tbstoreshoppingcarts.TotalDiscount
                        value_sale_order_line["discount_fixed"] = (
                            discount_price / item.Quantity
                        )

                    if item.ItemCalculatedSellPrice is None:
                        msg = (
                            f"Cart {tbstoreshoppingcarts.CartID} for item id"
                            f" {item.ItemID}, calculated value is None. Date"
                            f" {date_paid}"
                        )
                        _logger.warning(msg)
                        self.lst_warning.append(msg)
                    # if event_shopping_id:
                    #     value_sale_order_line[
                    #         "event_id"
                    #     ] = event_shopping_id.id
                    #     value_sale_order_line[
                    #         "event_ticket_id"
                    #     ] = event_ticket_shopping_id.id
                    #     value_sale_order_line[
                    #         "name"
                    #     ] = f"Ticket {event_shopping_id.name}"
                    # value_sale_order["tax_id"] = [(6, 0, self.sale_tax_id.ids)]
                    sale_order_line_id = env["sale.order.line"].create(
                        value_sale_order_line
                    )
                    # if event_registration_id:
                    #     event_registration_id.sale_order_line_id = (
                    #         sale_order_line_id.id
                    #     )
            # Validation amount is correct
            if (
                sale_order_id.amount_total - tbstoreshoppingcarts.TotalAmount
                > 0.1
            ):
                diff = (
                    sale_order_id.amount_total
                    - tbstoreshoppingcarts.TotalAmount
                )
                msg = (
                    f"Problème de calcul de {diff}$ pour shopping ID"
                    f" {tbstoreshoppingcarts.CartID}. Total calculé"
                    f" {sale_order_id.amount_total}$ est différent. Date"
                    f" {date_order}. Total amount"
                    f" {tbstoreshoppingcarts.TotalAmount}$. Total discount"
                    f" {tbstoreshoppingcarts.TotalDiscount}$"
                )
                _logger.error(msg)
                self.lst_error.append(msg)

            # Shipping
            # if sale_order_id.picking_ids:
            #     sale_order_id.picking_ids.button_validate()

            # Associate coupon
            # associate_coupon = [
            #     a
            #     for a in lst_tbl_tbstoreshoppingcartitemcoupons
            #     if a.CartItemID == obj_id_i
            # ]
            # if associate_coupon:
            #     sale_order_id
            #     print("es")
            # Create invoice
            # Validate sale order
            # sale_order_id.action_confirm()
            if MIGRATE_INVOICE:
                invoice_line = []
                for line in sale_order_id.order_line:
                    value_line = {
                        "product_id": line.product_id.id,
                        "name": line.name,
                        "quantity": line.product_uom_qty,
                        # "price_unit": line.price_unit,
                        "price_unit": line.price_subtotal
                        / line.product_uom_qty,
                        "account_id": env.ref(
                            "l10n_ca.ca_en_chart_template_en"
                        ).id,
                        "sale_line_ids": [(6, 0, line.ids)],
                        # "tax_ids": False,
                    }
                    # if USE_DISCOUNT_PERC:
                    #     value_line["discount"] = line.discount
                    # else:
                    #     value_line["discount_fixed"] = line.discount_fixed
                    invoice_line.append(
                        (
                            0,
                            0,
                            value_line,
                        )
                    )
                # Create Invoice
                invoice_vals = {
                    "move_type": "out_invoice",  # for customer invoice
                    "partner_id": sale_order_id.partner_id.id,
                    "journal_id": journal_sale_id.id,
                    "date": date_paid,
                    "invoice_date": date_paid,
                    "invoice_date_due": date_paid,
                    "invoice_origin": sale_order_id.name,
                    "currency_id": env.company.currency_id.id,
                    "company_id": env.company.id,
                    "invoice_line_ids": invoice_line,
                    "narration": '<p>Conditions générales : <a href="https://harmoniesante.com/terms" target="_blank" rel="noreferrer noopener">https://harmoniesante.com/terms</a> </p>',
                }

                invoice_id = env["account.move"].create(invoice_vals)

                # Migration message
                comment_message = (
                    "Transaction"
                    f" #{tbstoreshoppingcarts.ProviderTransactionID} {tbstoreshoppingcarts.ProviderStatusText}<br/>Date"
                    f" create {date_created}<br/>Date order"
                    f" {date_order}<br/>Date paid {date_paid}<br/>Total amount"
                    f" {tbstoreshoppingcarts.TotalAmount}. Total discount"
                    f" {tbstoreshoppingcarts.TotalDiscount}"
                )
                comment_value = {
                    "subject": (
                        "Note de migration - Plateforme ASP.net avant"
                        " migration - Commande No."
                        f" {tbstoreshoppingcarts.CartID}"
                    ),
                    "body": f"<p>{comment_message}</p>",
                    "parent_id": False,
                    "message_type": "comment",
                    "author_id": SUPERUSER_ID,
                    "model": "account.move",
                    "res_id": invoice_id.id,
                }
                env["mail.message"].create(comment_value)

                invoice_id.action_post()

                # Validate Invoice (optional)
                # sale_order_id.write({"invoice_ids": [(4, invoice_id.id)]})
                if invoice_id.amount_total > 0:
                    vals = {
                        "amount": invoice_id.amount_total,
                        "date": date_paid,
                        "partner_type": "customer",
                        "partner_id": sale_order_id.partner_id.id,
                        "payment_type": "inbound",
                        "payment_method_id": env.ref(
                            "account.account_payment_method_manual_in"
                        ).id,
                        "journal_id": journal_id.id,
                        "currency_id": env.company.currency_id.id,
                        "company_id": env.company.id,
                    }
                    payment_id = env["account.payment"].create(vals)
                    # invoice_id.write({"payment_id": payment_id.id})
                    payment_id.action_post()
                    payment_ml = payment_id.line_ids.filtered(
                        lambda l: l.account_id
                        == default_account_client_recv_id
                    )
                    res = invoice_id.with_context(
                        install_mode=True,
                        move_id=invoice_id.id,
                        line_id=payment_ml.id,
                        paid_amount=invoice_id.amount_total,
                    ).js_assign_outstanding_line(payment_ml.id)
                    if not payment_ml.reconciled:
                        msg = (
                            f"Facture non payé id {invoice_id.id}. Date"
                            f" {date_paid}"
                        )
                        _logger.warning(msg)
                        self.lst_warning.append(msg)
                    # partials = res.get("partials")
                    # if partials:
                    #     print(partials)
                    #     invoice_id.with_context(
                    #         paid_amount=invoice_id.amount_total
                    #     ).js_assign_outstanding_line(payment_ml.id)
                    # print(payment_ml.reconciled)
                    # print(invoice_id.amount_residual)
                    # print(invoice_id.payment_state)
                # invoice_id.action_post()
                # invoice_id.js_assign_outstanding_line(payment_id.id)
                # invoice_id._post()

                # Create invoice
                # new_invoice = sale_order_id._create_invoices()
                # Validate invoice
                # new_invoice.action_post()
                # new_invoice.invoice_origin = sale_order_id.name + ", 987 - " + self.name
                # invoice = sale_order_id.invoice_ids

            name = ""
            if DEBUG_OUTPUT:
                _logger.info(
                    f"{pos_id} - {model_name} - table {table_name} - ADDED"
                    f" '{name}' id {obj_id_i}. Date"
                    f" {date_paid}"
                )

    def migrate_tbTrainingCourses(self):
        """
        :return:
        """
        _logger.info("Migrate tbTrainingCourses")
        env = api.Environment(self.cr, SUPERUSER_ID, {})
        if self.dct_k_tbtrainingcourses_id_test_v_slide_channel:
            return
        table_name = f"{self.db_name}.dbo.tbTrainingCourses"
        lst_tbl_tbtrainingcourses = self.dct_tbl.get(table_name)
        table_courses_name = f"{self.db_name}.dbo.tbStoreItems"
        lst_tbl_tbcourses = self.dct_tbl.get(table_courses_name)
        # table_name = f"{self.db_name}.dbo.tbStoreItems"
        # lst_tbl_tbstoreitems = self.dct_tbl.get(table_name)
        lst_tbl_tbStoreItemTrainingCourses = self.dct_tbl.get(
            f"{self.db_name}.dbo.tbStoreItemTrainingCourses"
        )
        model_name = "slide.channel"
        pattern_formation_no = r"\b\d+\.\w+\b"

        if ENABLE_SELLER_MARKETPLACE:
            default_seller_id = self.dct_partner_id[DEFAULT_SELL_USER_ID]
            default_seller_id.seller = True
            default_seller_id.url_handler = default_seller_id.name.replace(
                " ", "_"
            )
        default_user_seller_id = self.dct_res_user_id[DEFAULT_SELL_USER_ID]

        for i, tbstoreitems in enumerate(lst_tbl_tbcourses):
            if DEBUG_LIMIT and i > LIMIT:
                self.dct_data_skip[table_courses_name] += (
                    len(lst_tbl_tbcourses) - i
                )
                break
            if tbstoreitems.CategoryID not in (1, 2):
                continue
            pos_id = f"{i + 1}/{len(lst_tbl_tbcourses)}"
            obj_id_i = tbstoreitems.ItemID
            name = tbstoreitems.ItemNameFR

            match = re.search(pattern_formation_no, name)
            if match:
                no_formation = match.group()
            else:
                msg = f"Cannot find no_formation into item {name}"
                _logger.warning(msg)
                self.lst_warning.append(msg)
                continue

            if (
                no_formation
                in self.dct_k_formation_name_v_slide_channel.keys()
            ):
                course_id = self.dct_k_formation_name_v_slide_channel.get(
                    no_formation
                )
                # Migration message
                comment_message = (
                    "Merge avec "
                    f" #{tbstoreitems.ItemID} {tbstoreitems.ItemNameFR}<br/>Date"
                    f" {tbstoreitems.DateCreated}"
                )
                comment_value = {
                    "subject": (
                        "Note de migration - Plateforme ASP.net avant migration -"
                        f" Item No. {tbstoreitems.ItemID} a été mergé."
                    ),
                    "body": f"<p>{comment_message}</p>",
                    "parent_id": False,
                    "message_type": "comment",
                    "author_id": SUPERUSER_ID,
                    "model": model_name,
                    "res_id": course_id.id,
                }
                env["mail.message"].create(comment_value)
            else:
                # self.dct_k_formation_name_v_product_template[no_formation].append(tbcourses)
                ignore = False
                for key_ignore in LST_KEY_EVENT:
                    if name.endswith(key_ignore.strip()):
                        msg = (
                            f"Ignore course ID {obj_id_i} name {name}. Will be en"
                            " event."
                        )
                        _logger.warning(msg)
                        self.lst_warning.append(msg)
                        ignore = True
                        break
                if ignore:
                    continue

                value = {
                    "name": name,
                    # "description": slide_channel.Description.strip(),
                    "is_published": True,
                    "visibility": "public",
                    "enroll": "payment",
                    "create_date": tbstoreitems.DateCreated,
                }
                if ENABLE_SELLER_MARKETPLACE:
                    value["seller_id"] = default_seller_id.id
                value["user_id"] = default_user_seller_id.id

                item_id = self.dct_k_tbstoreitems_v_product_template.get(
                    obj_id_i
                )
                if item_id:
                    value["product_id"] = item_id.id
                    value["image_1920"] = item_id.image_1920
                    value["name"] = item_id.name
                    value["description"] = item_id.description_sale
                    value["description_short"] = item_id.description_sale
                    value["description_html"] = item_id.website_description
                lst_training_courses = [
                    (pos_training_id, a)
                    for pos_training_id, a in enumerate(
                        lst_tbl_tbtrainingcourses
                    )
                    if a.CourseName.lower() in name.lower()
                ]
                if not lst_training_courses:
                    # Disable it
                    value["active"] = False

                obj_slide_channel_id = env[model_name].create(value)
                self.dct_k_formation_name_v_slide_channel[no_formation] = (
                    obj_slide_channel_id
                )
                # Migration message
                comment_message = (
                    "Migration item"
                    f" #{tbstoreitems.ItemID} {tbstoreitems.ItemNameFR}<br/>Date"
                    f" {tbstoreitems.DateCreated}"
                )
                comment_value = {
                    "subject": (
                        "Note de migration - Plateforme ASP.net avant migration -"
                        f" Item No. {tbstoreitems.ItemID}"
                    ),
                    "body": f"<p>{comment_message}</p>",
                    "parent_id": False,
                    "message_type": "comment",
                    "author_id": SUPERUSER_ID,
                    "model": model_name,
                    "res_id": obj_slide_channel_id.id,
                }
                env["mail.message"].create(comment_value)

                # Support courses with attestation
                if lst_training_courses:
                    if len(lst_training_courses) > 1:
                        msg = f"Double course name {name}."
                        _logger.warning(msg)
                        self.lst_warning.append(msg)
                    pos_training_id, tbtrainingcourses = lst_training_courses[
                        0
                    ]
                    obj_training_id_i = tbtrainingcourses.CourseID
                    self.dct_k_tbtrainingcourses_id_test_v_slide_channel[
                        tbtrainingcourses.TestID
                    ] = obj_slide_channel_id
                    self.dct_k_tbtrainingcourses_v_slide_channel[
                        obj_training_id_i
                    ] = obj_slide_channel_id
                    if DEBUG_OUTPUT:
                        _logger.info(
                            f"{pos_training_id} - {model_name} - table {table_name} - ADDED"
                            f" '{name}' id {obj_training_id_i}"
                        )
            if DEBUG_OUTPUT:
                _logger.info(
                    f"{pos_id} - {model_name} - table {table_courses_name} - ADDED"
                    f" '{name}' id {obj_id_i}"
                )

        # for i, tbtrainingcourses in enumerate(lst_tbl_tbtrainingcourses):
        #     if DEBUG_LIMIT and i > LIMIT:
        #         self.dct_data_skip[table_name] += (
        #             len(lst_tbl_tbtrainingcourses) - i
        #         )
        #         break
        #
        #     pos_id = f"{i+1}/{len(lst_tbl_tbtrainingcourses)}"
        #
        #     # Slide Channel
        #     # TODO Duration -> create a statistics, check _compute_slides_statistics
        #     # TODO ReleaseDate
        #     obj_id_i = tbtrainingcourses.CourseID
        #     name = tbtrainingcourses.CourseName
        #     # obj_id_i = tbtrainingcourses.ItemID
        #     # name = tbtrainingcourses.ItemNameFR
        #
        #     ignore = False
        #     for key_ignore in LST_KEY_EVENT:
        #         if name.endswith(key_ignore.strip()):
        #             msg = (
        #                 f"Ignore course ID {obj_id_i} name {name}. Will be en"
        #                 " event."
        #             )
        #             _logger.warning(msg)
        #             self.lst_warning.append(msg)
        #             ignore = True
        #             break
        #     if ignore:
        #         continue
        #
        #     value = {
        #         "name": name,
        #         # "description": slide_channel.Description.strip(),
        #         "is_published": True,
        #         "visibility": "public",
        #         "enroll": "payment",
        #         "create_date": tbtrainingcourses.CreatedDate,
        #     }
        #     if ENABLE_SELLER_MARKETPLACE:
        #         value["seller_id"] = default_seller_id.id
        #         value["user_id"] = default_user_seller_id.id
        #
        #     product_course_id = [
        #         a
        #         for a in lst_tbl_tbStoreItemTrainingCourses
        #         if a.CourseID == obj_id_i
        #     ]
        #     if product_course_id:
        #         item_id = self.dct_k_tbstoreitems_v_product_template.get(
        #             product_course_id[0].ItemID
        #         )
        #         if item_id:
        #             value["product_id"] = item_id.id
        #             value["image_1920"] = item_id.image_1920
        #             value["name"] = item_id.name
        #             value["description"] = item_id.description_sale
        #             value["description_short"] = item_id.description_sale
        #             value["description_html"] = item_id.website_description
        #     obj_slide_channel_id = env[model_name].create(value)
        #
        #     self.dct_k_tbtrainingcourses_id_test_v_slide_channel[
        #         tbtrainingcourses.TestID
        #     ] = obj_slide_channel_id
        #     self.dct_k_tbtrainingcourses_v_slide_channel[obj_id_i] = (
        #         obj_slide_channel_id
        #     )
        #     if DEBUG_OUTPUT:
        #         _logger.info(
        #             f"{pos_id} - {model_name} - table {table_name} - ADDED"
        #             f" '{name}' id {obj_id_i}"
        #         )

    def continue_migrate_tbTrainingCourses_knowledge_question(
        self, test_id_tbl
    ):
        default_user_seller_id = self.dct_res_user_id[DEFAULT_SELL_USER_ID]
        env = api.Environment(self.cr, SUPERUSER_ID, {})

        lbl_knowledge_test = f"{self.db_name}.dbo.tbKnowledgeTests"
        lst_tbl_knowledge_test = self.dct_tbl.get(lbl_knowledge_test)
        lst_knowledge_test_tbl = [
            a for a in lst_tbl_knowledge_test if a.TestID == test_id_tbl
        ]
        if not lst_knowledge_test_tbl:
            msg = f"About tbKnowledgeTests, missing TestID {test_id_tbl}"
            _logger.warning(msg)
            self.lst_warning.append(msg)
            self.dct_data_skip[lbl_knowledge_test] += 1
            return False, False
        knowledge_test_tbl = lst_knowledge_test_tbl[0]

        # Survey.question init
        lbl_knowledge_question = f"{self.db_name}.dbo.tbKnowledgeQuestions"
        lst_tbl_knowledge_question = self.dct_tbl.get(lbl_knowledge_question)
        lst_knowledge_question_tbl = [
            a for a in lst_tbl_knowledge_question if a.TestID == test_id_tbl
        ]
        if not lst_knowledge_question_tbl:
            msg = f"About tbKnowledgeQuestions, missing TestID {test_id_tbl}"
            _logger.warning(msg)
            self.lst_warning.append(msg)
            self.dct_data_skip[lbl_knowledge_question] += 1
            return False, False

        # Survey.question.answer init
        lbl_knowledge_question_answer = (
            f"{self.db_name}.dbo.tbKnowledgeAnswerChoices"
        )
        lst_tbl_knowledge_question_answer = self.dct_tbl.get(
            lbl_knowledge_question_answer
        )

        # Survey.survey create
        # TODO if enable certification_give_badge, need to create gamification.badge and associate to certification_badge_id
        value_survey_survey = {
            "title": knowledge_test_tbl.TestName,
            "certification": True,
            # "certification_give_badge": True,
            "scoring_type": "scoring_with_answers",
            "user_id": default_user_seller_id.id,
        }
        obj_survey = env["survey.survey"].create(value_survey_survey)
        self.dct_k_knowledgetest_v_survey_id[knowledge_test_tbl.TestID] = (
            obj_survey
        )

        for knowledge_question_tbl in lst_knowledge_question_tbl:
            # ignore EN like QuestionEN and SubjectEN
            # TODO SubjectFR
            base_qvalues = {
                "sequence": knowledge_question_tbl.QuestionOrder + 9,
                "title": knowledge_question_tbl.QuestionFR,
                "survey_id": obj_survey.id,
            }
            if knowledge_question_tbl.SubjectFR:
                base_qvalues["description"] = (
                    f"<p>{knowledge_question_tbl.SubjectFR}</p>"
                )
            question_id = env["survey.question"].create(base_qvalues)
            self.dct_k_tbknowledgequestions_v_survey_question[
                knowledge_question_tbl.QuestionID
            ] = question_id

            # Continue Survey.question.answer
            tbl_knowledge_question_id = knowledge_question_tbl.QuestionID
            lst_knowledge_question_answer_tbl = [
                a
                for a in lst_tbl_knowledge_question_answer
                if a.QuestionID == tbl_knowledge_question_id
            ]
            if not lst_knowledge_question_answer_tbl:
                msg = (
                    "About tbKnowledgeAnswerChoices, missing"
                    f" QuestionID {tbl_knowledge_question_id}"
                )
                _logger.warning(msg)
                self.lst_warning.append(msg)
                self.dct_data_skip[lbl_knowledge_question_answer] += 1
                continue
            # TODO AnswerEN
            for (
                knowledge_question_answer_tbl
            ) in lst_knowledge_question_answer_tbl:
                sequence = knowledge_question_answer_tbl.AnswerOrder + 9
                value_answer = {
                    "sequence": sequence,
                    "value": knowledge_question_answer_tbl.AnswerFR,
                    "is_correct": knowledge_question_answer_tbl.IsRightAnswer,
                    "question_id": question_id.id,
                    "answer_score": (
                        10
                        if knowledge_question_answer_tbl.IsRightAnswer
                        else 0
                    ),
                }
                question_answer_id = env["survey.question.answer"].create(
                    value_answer
                )
                self.dct_k_tbknowledgeanswerresults_v_survey_question_answer[
                    knowledge_question_answer_tbl.AnswerID
                ] = question_answer_id
        return obj_survey, knowledge_test_tbl

    def continue_migrate_tbTrainingCourses_slide_slide(
        self,
        knowledge_test_tbl,
        obj_slide_channel_id,
        obj_survey,
    ):
        env = api.Environment(self.cr, SUPERUSER_ID, {})
        default_user_seller_id = self.dct_res_user_id[DEFAULT_SELL_USER_ID]

        # Create slide.slide
        ticks = knowledge_test_tbl.TrainingDuration
        td = datetime.timedelta(microseconds=ticks / 10)
        days, hours, minutes = (
            td.days,
            td.seconds // 3600,
            td.seconds % 3600 / 60.0,
        )
        time_duration_hour = hours

        if link_generic_video_demo:
            value_slide = {
                "name": f"Vidéo : {knowledge_test_tbl.TestName}",
                "channel_id": obj_slide_channel_id.id,
                "slide_category": "video",
                "document_type": "url",
                "video_url": link_generic_video_demo,
                # "description": knowledge_test_tbl.CertificateBodyFR,
                "survey_id": obj_survey.id,
                "is_published": True,
                "website_published": True,
                "completion_time": time_duration_hour,
                "create_date": knowledge_test_tbl.DateCreated,
                "user_id": default_user_seller_id.id,
            }
            obj_slide = (
                env["slide.slide"]
                .with_context(website_slides_skip_fetch_metadata=True)
                .create(value_slide)
            )
            # Bug, auto switch
            obj_slide.slide_category = "video"
        # TODO Subject
        # TODO TestKey ??
        # TODO Trainer - abandon, do it manually
        # is compute later PassingGrade
        value_slide = {
            "name": f"Certification : {knowledge_test_tbl.TestName}",
            "channel_id": obj_slide_channel_id.id,
            "slide_category": "certification",
            "slide_type": "certification",
            # "description": knowledge_test_tbl.CertificateBodyFR,
            "survey_id": obj_survey.id,
            "is_published": True,
            "website_published": True,
            "completion_time": 0.25,
            "create_date": knowledge_test_tbl.DateCreated,
            "user_id": default_user_seller_id.id,
        }
        obj_slide = env["slide.slide"].create(value_slide)
        self.dct_k_survey_v_slide_survey_id[obj_survey.id] = obj_slide
        return obj_slide

    def continue_migrate_tbTrainingCourses_knownledge_answer(self):
        env = api.Environment(self.cr, SUPERUSER_ID, {})

        table_name_knowledge_test_results = (
            f"{self.db_name}.dbo.tbKnowledgeTestResults"
        )
        lst_tbl_knowledge_test_results = self.dct_tbl.get(
            table_name_knowledge_test_results
        )
        # Import result survey
        for tbl_knowledge_test_results in lst_tbl_knowledge_test_results:
            partner_id = self.dct_partner_id.get(
                tbl_knowledge_test_results.UserID
            )
            if not partner_id:
                msg = (
                    "Cannot find partner_id for UserID"
                    f" '{tbl_knowledge_test_results.UserID}'"
                )
                _logger.error(msg)
                self.lst_error.append(msg)
                self.dct_data_skip[table_name_knowledge_test_results] += 1
                continue

            obj_survey = self.dct_k_knowledgetest_v_survey_id.get(
                tbl_knowledge_test_results.TestID
            )
            if not obj_survey:
                msg = (
                    "Cannot find survey for TestID"
                    f" '{tbl_knowledge_test_results.TestID}'"
                )
                _logger.error(msg)
                self.lst_error.append(msg)
                self.dct_data_skip[table_name_knowledge_test_results] += 1
                continue
            # DONE Ignore Grade, will be recalcul, validate the value is good by a warning
            # DONE validate IsSuccessful
            # TODO start date and end date is the same
            # DONE last_displayed_page_id select last question id
            obj_slide = self.dct_k_survey_v_slide_survey_id[obj_survey.id]

            # Create partner input survey
            value_survey_user_input = {
                "survey_id": obj_survey.id,
                "create_date": tbl_knowledge_test_results.DateCreated,
                "start_datetime": tbl_knowledge_test_results.DateCreated,
                "end_datetime": tbl_knowledge_test_results.DateCreated,
                "state": "done",
                "email": partner_id.email,
                "nickname": partner_id.name,
                "partner_id": partner_id.id,
                # "last_displayed_page_id": 1,
                "slide_id": obj_slide.id,
            }
            obj_survey_user_input = env["survey.user_input"].create(
                value_survey_user_input
            )
            table_name_knowledge_answer_results = (
                f"{self.db_name}.dbo.tbKnowledgeAnswerResults"
            )
            lst_tbl_knowledge_answer_results = self.dct_tbl.get(
                table_name_knowledge_answer_results
            )
            # Get associate result line
            lst_associate_answer_result = [
                a
                for a in lst_tbl_knowledge_answer_results
                if a.TestResultID == tbl_knowledge_test_results.TestResultID
            ]
            survey_question_answer = None
            obj_survey_user_input_line = None
            for associate_answer_result in lst_associate_answer_result:
                try:
                    survey_question_answer = self.dct_k_tbknowledgeanswerresults_v_survey_question_answer[
                        associate_answer_result.AnswerID
                    ]
                except Exception as e:
                    msg = (
                        "Cannot retrieve answer ID"
                        f" {associate_answer_result.AnswerID} for"
                        " survey_question_answer."
                    )
                    _logger.error(msg)
                    self.lst_error.append(msg)
                    self.dct_data_skip[
                        table_name_knowledge_answer_results
                    ] += 1
                    continue
                value_survey_user_input_line = {
                    "user_input_id": obj_survey_user_input.id,
                    "question_id": survey_question_answer.question_id.id,
                    "answer_type": "suggestion",
                    "create_date": tbl_knowledge_test_results.DateCreated,
                    "suggested_answer_id": survey_question_answer.id,
                    "answer_is_correct": survey_question_answer.is_correct,
                    "answer_score": (
                        10 if survey_question_answer.is_correct else 0
                    ),
                }
                obj_survey_user_input_line = env[
                    "survey.user_input.line"
                ].create(value_survey_user_input_line)
            # Save last question answered
            if (
                survey_question_answer is not None
                and obj_survey_user_input_line is not None
            ):
                obj_survey_user_input.last_displayed_page_id = (
                    survey_question_answer.question_id.id
                )
            # Fill channel partner to show certification complete
            completed = obj_survey_user_input.scoring_success
            value_slide_channel_partner = {
                "channel_id": obj_slide.channel_id.id,
                "completion": 100 if completed else 0,
                "completed_slides_count": 1 if completed else 0,
                "completed": completed,
                "partner_id": partner_id.id,
                "create_date": tbl_knowledge_test_results.DateCreated,
            }
            # Validate if exist
            obj_slide_channel_partner = env["slide.channel.partner"].search(
                [
                    ("partner_id", "=", partner_id.id),
                    ("channel_id", "=", obj_slide.channel_id.id),
                ],
                limit=1,
            )
            if obj_slide_channel_partner:
                if obj_slide_channel_partner.completion != 100 and completed:
                    obj_slide_channel_partner.completion = 100
                    # _logger.info(
                    #     "Increase value complete to 100 for partner id"
                    #     f" {partner_id.id}"
                    # )
                # else:
                #     obj_slide_channel_partner.completion = 0
            else:
                obj_slide_channel_partner = env[
                    "slide.channel.partner"
                ].create(value_slide_channel_partner)

            # Create slide.slide.partner
            # Validate if exist
            obj_slide_partner = env["slide.slide.partner"].search(
                [
                    ("partner_id", "=", partner_id.id),
                    ("slide_id", "=", obj_slide.id),
                ],
                limit=1,
            )
            if not obj_slide_partner:
                value_slide_partner = {
                    "create_date": tbl_knowledge_test_results.DateCreated,
                    "slide_id": obj_slide.id,
                    "partner_id": partner_id.id,
                    "completed": completed,
                }
                obj_slide_partner = env["slide.slide.partner"].create(
                    value_slide_partner
                )
            else:
                if not obj_slide_partner.completed and completed:
                    obj_slide_partner.completed = True

    def migrate_tbUsers(self):
        """
        :return:
        """
        _logger.info("Migrate tbUsers")
        env = api.Environment(self.cr, SUPERUSER_ID, {})
        if self.dct_tbusers:
            return
        dct_tbusers = {}
        table_name = f"{self.db_name}.dbo.tbUsers"
        lst_tbl_tbusers = self.dct_tbl.get(table_name)
        mailing_list_id = env.ref("mass_mailing.mailing_list_data")
        model_name = "res.partner"

        for i, tbusers in enumerate(lst_tbl_tbusers):
            if DEBUG_LIMIT and i > LIMIT:
                self.dct_data_skip[table_name] += len(lst_tbl_tbusers) - i
                break

            pos_id = f"{i+1}/{len(lst_tbl_tbusers)}"

            # Ignore user
            if tbusers.UserID == 1231:
                self.dct_data_skip[table_name] += 1
                continue

            obj_id_i = tbusers.UserID
            name = tbusers.FullName
            email = tbusers.Email.lower().strip()
            user_name = tbusers.UserName.lower().strip()

            if email != user_name:
                msg = (
                    f"User name '{user_name}' is different from email"
                    f" '{email}'"
                )
                _logger.warning(msg)
                self.lst_warning.append(msg)
            if not user_name:
                msg = f"Missing user name for membre {tbusers}"
                _logger.error(msg)
                self.lst_error.append(msg)

            # Country mapping
            dct_country_mapping = {
                1: 38,
                3: 75,
                11: 7,
                23: 20,
                32: 29,
                45: 43,
                111: 109,
                135: 133,
                179: 177,
                189: 187,
            }
            dct_province_mapping = {
                2: 534,
                5: 536,
                8: 541,
                9: 543,
                12: 538,
                13: 545,
                33: 28,
                35: 42,
                45: 34,
                52: 47,
                58: 53,
                66: -1,  # Martinique is a country
                69: 1675,
                72: -1,  # Haute-Normandie merge to Normandie
                76: 1677,  # Hauts-de-France
                77: 1668,  # Lorraine merge to Grand Est
                78: 1668,  # Alsace merge to Grand Est
                80: 1679,
                81: 1672,
                82: 1019,
                83: 1669,  # Merge to Nouvelle-Aquitaine
                86: 1669,  # Merge to Nouvelle-Aquitaine
                88: 1676,  # Occitanie
                89: 1680,
            }
            # Fix country
            country_id = dct_country_mapping[tbusers.CountryID]
            state_id = dct_province_mapping[tbusers.ProvinceID]
            if state_id == -1:
                if tbusers.ProvinceID == 66:
                    country_id = 149
                    state_id = False
                if tbusers.ProvinceID == 72:
                    state_id = 1668

            # TODO IsAnimator is internal member, else only portal member
            # Info ignore DisplayName, FirstName, Gender, ProperName, LastName, ProviderUserKey, UserId
            value = {
                "name": name,
                "email": email,
                "state_id": state_id,
                "country_id": country_id,
                # "gender": "female" if tbusers.Gender else "male",
                "tz": "America/Montreal",
                "create_date": tbusers.CreatedDate,
            }

            if tbusers.Occupation and tbusers.Occupation not in ["xxx"]:
                value["function"] = tbusers.Occupation

            # if tbusers.DateOfBirth:
            #     value["birthdate_date"] = tbusers.DateOfBirth

            if tbusers.AddressLine1 and tbusers.AddressLine1.strip():
                value["street"] = tbusers.AddressLine1.strip()
            if tbusers.AddressLine2 and tbusers.AddressLine2.strip():
                value["street2"] = tbusers.AddressLine2.strip()
            if tbusers.PostalCode and tbusers.PostalCode.strip():
                value["zip"] = tbusers.PostalCode.strip()
            if tbusers.City and tbusers.City.strip():
                value["city"] = tbusers.City.strip()
            if tbusers.WebSite and tbusers.WebSite.strip():
                value["website"] = tbusers.WebSite.strip()
            if tbusers.HomePhone and tbusers.HomePhone.strip():
                value["phone"] = tbusers.HomePhone.strip()
            if tbusers.WorkPhone and tbusers.WorkPhone.strip():
                value["mobile"] = tbusers.WorkPhone.strip()

            obj_partner_id = env[model_name].create(value)
            dct_tbusers[obj_id_i] = obj_partner_id
            self.dct_partner_id[obj_id_i] = obj_partner_id

            if DEBUG_OUTPUT:
                _logger.info(
                    f"{pos_id} - {model_name} - table {table_name} - ADDED"
                    f" '{name}' '{email}' id {obj_id_i}"
                )

            # Add to mailing list
            if tbusers.ReceiveNewsletter:
                value_mailing_list_contact = {
                    "name": name,
                    "email": email,
                    "list_ids": [(4, mailing_list_id.id)],
                }
                env["mailing.contact"].create(value_mailing_list_contact)

            # Add message about migration information
            comment_message = (
                "Date de création :"
                f" {tbusers.CreatedDate.strftime('%d/%m/%Y %H:%M:%S')}<br/>"
            )
            if tbusers.LastUpdatedDate:
                comment_message += (
                    "Dernière modification :"
                    f" {tbusers.LastUpdatedDate.strftime('%d/%m/%Y %H:%M:%S')}<br/>"
                )
            if tbusers.DateOfBirth:
                comment_message += (
                    "Date de naissance :"
                    f" {tbusers.DateOfBirth.strftime('%d/%m/%Y')}<br/>"
                )
            if tbusers.IsAnimator:
                comment_message += f"Est un animateur<br/>"
            if tbusers.Occupation:
                comment_message += f"Occupation : {tbusers.Occupation}<br/>"
            comment_value = {
                "subject": (
                    "Note de migration - Plateforme ASP.net avant migration"
                ),
                "body": f"<p>{comment_message}</p>",
                "parent_id": False,
                "message_type": "comment",
                "author_id": SUPERUSER_ID,
                "model": "res.partner",
                "res_id": obj_partner_id.id,
            }
            env["mail.message"].create(comment_value)

            if obj_id_i == DEFAULT_SELL_USER_ID:
                value = {
                    "name": obj_partner_id.name,
                    "active": True,
                    "login": email,
                    "email": email,
                    # "groups_id": groups_id,
                    # "company_id": company_id.id,
                    # "company_ids": [(4, company_id.id)],
                    "partner_id": obj_partner_id.id,
                }

                obj_user = (
                    env["res.users"]
                    # .with_context(
                    #     {"no_reset_password": no_reset_password}
                    # )
                    .create(value)
                )

                self.dct_res_user_id[obj_id_i] = obj_user
            else:
                # Give portal access
                wizard_all = (
                    env["portal.wizard"]
                    .with_context(**{"active_ids": [obj_partner_id.id]})
                    .create({})
                )
                wizard_all.user_ids.action_grant_access()

        self.dct_tbusers = dct_tbusers
