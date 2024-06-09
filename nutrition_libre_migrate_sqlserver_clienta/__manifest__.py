# License AGPL-3.0 or later (http://www.gnu.org/licenses/agpl).

{
    "name": "Nutrition Libre Migrate sqlserver for ClientA",
    "version": "16.0.0.1",
    "author": "TechnoLibre",
    "website": "https://technolibre.ca",
    "license": "AGPL-3",
    "category": "Extra tools",
    "summary": "Migrate database of project clienta",
    "description": """
Migrate sqlserver for ClientA
=============================
""",
    "depends": [
        "website_slides",
        "survey",
        "event_sale",
        "mass_mailing",
        "loyalty",
        "sale_fixed_discount",
        "website_sale_slides",
        "stock",
        "sale_stock",
    ],
    "external_dependencies": {
        "python": [
            "pymssql",
        ],
    },
    "post_init_hook": "post_init_hook",
    "installable": True,
}
