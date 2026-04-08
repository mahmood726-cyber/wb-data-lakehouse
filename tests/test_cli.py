"""Tests for CLI argument parsing."""
from wb_data_lakehouse.cli import build_parser


def test_parser_fetch_domain():
    parser = build_parser()
    args = parser.parse_args(["fetch", "wdi", "--skip-existing"])
    assert args.command == "fetch"
    assert args.domain == "wdi"
    assert args.skip_existing is True


def test_parser_fetch_all():
    parser = build_parser()
    args = parser.parse_args(["fetch", "--all"])
    assert args.all is True


def test_parser_promote_domain():
    parser = build_parser()
    args = parser.parse_args(["promote", "hnp"])
    assert args.command == "promote"
    assert args.domain == "hnp"


def test_parser_promote_all():
    parser = build_parser()
    args = parser.parse_args(["promote", "--all"])
    assert args.command == "promote"
    assert args.all is True


def test_parser_search():
    parser = build_parser()
    args = parser.parse_args(["search", "mortality", "--domain", "hnp"])
    assert args.keyword == "mortality"
    assert args.domain == "hnp"


def test_parser_status():
    parser = build_parser()
    args = parser.parse_args(["status"])
    assert args.command == "status"


def test_parser_registry_list():
    parser = build_parser()
    args = parser.parse_args(["registry-list"])
    assert args.command == "registry-list"


def test_parser_catalog():
    parser = build_parser()
    args = parser.parse_args(["catalog"])
    assert args.command == "catalog"
