#!/usr/bin/env python3
"""兼容入口：统一转发到新的资产训练编排脚本。"""

from train_asset_suite import main


if __name__ == '__main__':
    raise SystemExit(main())
