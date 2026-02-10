"""
Update the sources registered in lightcurvedb to match those from socat.
"""


def main():
    from socat.client import settings as socat_settings

    from lightcurvedb.config import settings as lightcurvedb_settings
    from lightcurvedb.integrations.socat import upsert_sources

    manager = lightcurvedb_settings.sync_manager()
    manager.create_all()

    settings = socat_settings.SOCatClientSettings()

    with manager.session() as session:
        upsert_sources(client=settings.client, session=session, progress_bar=True)


if __name__ == "__main__":
    main()
