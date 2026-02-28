"""Add ondelete SET NULL to connector FKs on mapping_documents

Revision ID: a1b2c3d4e5f6
Revises: 3c5059432bb2
Create Date: 2026-02-28 14:13:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a1b2c3d4e5f6'
down_revision: Union[str, Sequence[str], None] = '3c5059432bb2'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Recreate source/target connector FK constraints with ondelete=SET NULL.

    For SQLite: uses batch_alter_table (which rebuilds the table, so no
    constraint name issues).

    For PostgreSQL: uses raw SQL with explicit constraint names. We look up
    the actual constraint names from information_schema so the migration
    works whether the schema was created by create_all (auto-named FKs) or
    by a previous Alembic migration.
    """
    bind = op.get_bind()
    dialect = bind.dialect.name

    if dialect == "postgresql":
        # Look up the real FK constraint names from information_schema
        result = bind.execute(sa.text("""
            SELECT constraint_name, column_name
            FROM information_schema.key_column_usage
            WHERE table_name = 'mapping_documents'
              AND table_schema = 'public'
              AND constraint_name IN (
                  SELECT constraint_name FROM information_schema.referential_constraints
                  WHERE constraint_schema = 'public'
              )
              AND column_name IN ('source_connector_id', 'target_connector_id')
        """))
        rows = result.fetchall()

        for row in rows:
            constraint_name = row[0]
            bind.execute(sa.text(
                f'ALTER TABLE mapping_documents DROP CONSTRAINT IF EXISTS "{constraint_name}"'
            ))

        # Recreate both FKs with explicit names and ON DELETE SET NULL
        bind.execute(sa.text("""
            ALTER TABLE mapping_documents
                ADD CONSTRAINT fk_mapping_documents_source_connector_id
                FOREIGN KEY (source_connector_id) REFERENCES connectors(id)
                ON DELETE SET NULL
        """))
        bind.execute(sa.text("""
            ALTER TABLE mapping_documents
                ADD CONSTRAINT fk_mapping_documents_target_connector_id
                FOREIGN KEY (target_connector_id) REFERENCES connectors(id)
                ON DELETE SET NULL
        """))

    else:
        # SQLite: batch mode rebuilds the table, so no constraint-name issues
        with op.batch_alter_table('mapping_documents', schema=None) as batch_op:
            batch_op.create_foreign_key(
                'fk_mapping_documents_source_connector_id',
                'connectors',
                ['source_connector_id'],
                ['id'],
                ondelete='SET NULL',
            )
            batch_op.create_foreign_key(
                'fk_mapping_documents_target_connector_id',
                'connectors',
                ['target_connector_id'],
                ['id'],
                ondelete='SET NULL',
            )


def downgrade() -> None:
    """Revert FK constraints to plain references (no ondelete behaviour)."""
    bind = op.get_bind()
    dialect = bind.dialect.name

    if dialect == "postgresql":
        bind.execute(sa.text(
            'ALTER TABLE mapping_documents DROP CONSTRAINT IF EXISTS fk_mapping_documents_source_connector_id'
        ))
        bind.execute(sa.text(
            'ALTER TABLE mapping_documents DROP CONSTRAINT IF EXISTS fk_mapping_documents_target_connector_id'
        ))
        bind.execute(sa.text("""
            ALTER TABLE mapping_documents
                ADD CONSTRAINT fk_mapping_documents_source_connector_id
                FOREIGN KEY (source_connector_id) REFERENCES connectors(id)
        """))
        bind.execute(sa.text("""
            ALTER TABLE mapping_documents
                ADD CONSTRAINT fk_mapping_documents_target_connector_id
                FOREIGN KEY (target_connector_id) REFERENCES connectors(id)
        """))
    else:
        with op.batch_alter_table('mapping_documents', schema=None) as batch_op:
            batch_op.drop_constraint('fk_mapping_documents_source_connector_id', type_='foreignkey')
            batch_op.drop_constraint('fk_mapping_documents_target_connector_id', type_='foreignkey')
            batch_op.create_foreign_key(
                'fk_mapping_documents_source_connector_id',
                'connectors',
                ['source_connector_id'],
                ['id'],
            )
            batch_op.create_foreign_key(
                'fk_mapping_documents_target_connector_id',
                'connectors',
                ['target_connector_id'],
                ['id'],
            )
