import React from "react";

const FeatureServiceIcon = ({
  size,
  className,
}: {
  size: number;
  className?: string;
}) => {
  return (
    <svg
      className={className}
      width={size}
      height={size}
      viewBox="0 0 32 32"
      fill="none"
      xmlns="http://www.w3.org/2000/svg"
    >
      <path
        d="M18 1.15L15.9 0L13.9 1.15L4.55 19L15.9 25L21.65 21.7L20.25 18.95L15.9 21.45L8.8 17.7L15.9 3.95L25.6 22.85L15.9 28.4L3.4 21.25L2 23.95L15.9 32L29.75 24L18 1.15Z"
        fill="#0569EA"
      />
    </svg>
  );
};

const FeatureServiceIcon16 = () => {
  return (
    <FeatureServiceIcon size={16} className="euiSideNavItemButton__icon" />
  );
};

const FeatureServiceIcon32 = () => {
  return (
    <FeatureServiceIcon
      size={32}
      className="euiIcon euiIcon--xLarge euiPageHeaderContent__titleIcon"
    />
  );
};

export { FeatureServiceIcon, FeatureServiceIcon16, FeatureServiceIcon32 };
