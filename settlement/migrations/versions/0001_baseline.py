"""基线：供应商重传产生的原始结算明细与收货行

Revision ID: 0001
Revises:
Create Date: 2026-11-26

明细的单号、金额、数量都按供应商上送的原始文本保存；收货行通过
带 ON DELETE CASCADE 的外键引用各次入库拿到的明细 ID。
"""

from alembic import op
import sqlalchemy as sa

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "settlement_detail",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("supplier_code", sa.Text(), nullable=False),
        sa.Column("order_no", sa.Text(), nullable=False),
        sa.Column("amount_text", sa.Text(), nullable=False),
        sa.Column("quantity_text", sa.Text(), nullable=False),
    )
    op.create_table(
        "receipt_line",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("detail_id", sa.Integer(), nullable=False),
        sa.Column("product_code", sa.Text(), nullable=False),
        sa.Column("received_qty", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["detail_id"],
            ["settlement_detail.id"],
            name="fk_receipt_line_detail_id",
            ondelete="CASCADE",
        ),
    )
    op.create_table(
        "receive_batch",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("batch_no", sa.Text(), nullable=False),
        sa.Column("note", sa.Text(), nullable=True),
    )


def downgrade():
    op.drop_table("receive_batch")
    op.drop_table("receipt_line")
    op.drop_table("settlement_detail")
