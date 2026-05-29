"""检查自适应调参建议并可选应用"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from reviews.reflection import ReflectionLearner
from trader.profile_config import get_active_profile, apply_threshold_adjustment

profile = get_active_profile()
print("current profile:", profile["name"])
print("current decision_threshold:", profile["decision_threshold"])

learner = ReflectionLearner()
sug = learner.suggest_threshold_adjustment(days=30, min_samples=5)
learner.close()
print("5d samples:", sug["total_5d"])
print("5d error_rate:", sug["error_rate"])
print("action:", sug["action"], "| delta:", sug["delta"])
print("new_threshold:", sug["new_threshold"])
print("reason:", sug["reason"])

if sug["action"] != "hold" and sug["delta"] != 0.0:
    print("\nApplying threshold adjustment...")
    updated = apply_threshold_adjustment(sug["new_threshold"], sug["reason"])
    print("Updated decision_threshold:", updated["decision_threshold"])
else:
    print("\nNo adjustment needed.")
