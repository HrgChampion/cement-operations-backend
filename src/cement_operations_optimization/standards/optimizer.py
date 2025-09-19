from standards_loader import STANDARDS

def check_compliance(cement_type: str, features: dict):
    """Compare plant features against BIS standards and return compliance + suggestions"""
    if cement_type not in STANDARDS:
        return {"error": f"No standards found for {cement_type}"}

    standards = STANDARDS[cement_type]
    compliance = {}
    suggestions = []

    for param, limits in standards.items():
        if param not in features:
            continue

        value = features[param]
        min_v = limits.get("min")
        max_v = limits.get("max")

        if min_v is not None and value < min_v:
            compliance[param] = False
            suggestions.append(f"{param} ({value}) below standard ({min_v}) → increase it.")
        elif max_v is not None and value > max_v:
            compliance[param] = False
            suggestions.append(f"{param} ({value}) above standard ({max_v}) → reduce it.")
        else:
            compliance[param] = True

    return {
        "cement_type": cement_type,
        "compliance": compliance,
        "suggestions": suggestions or ["All parameters within BIS standard limits ✅"]
    }
