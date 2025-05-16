package services

func (feast *FeastServices) getModeForType(feastType FeastServiceType) string {
	switch feastType {
	case RegistryFeastType:
		if feast.Handler.FeatureStore.Spec.Services.Registry != nil &&
			feast.Handler.FeatureStore.Spec.Services.Registry.Local != nil &&
			feast.Handler.FeatureStore.Spec.Services.Registry.Local.Server != nil {
			mode := feast.Handler.FeatureStore.Spec.Services.Registry.Local.Server.Mode
			if mode != "" {
				return mode
			}
		}
		// fallback to rest mode for registry only
		return "rest"
	}
	return ""
}
