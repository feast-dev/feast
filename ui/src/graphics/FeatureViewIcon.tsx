import React from "react";

const FeatureViewIcon = ({
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
        fillRule="evenodd"
        clipRule="evenodd"
        d="M3.68657 14.2132L15.8385 21.2324L27.9904 14.2132L29.677 15.1874V17.0359L15.8385 25.0292L2 17.0359V15.1874L3.68657 14.2132Z"
        fill="#0569EA"
      />
      <path
        fillRule="evenodd"
        clipRule="evenodd"
        d="M3.68657 21.1824L15.8385 28.2015L27.9904 21.1824L29.677 22.1566V24.005L15.8385 31.9984L2 24.005V22.1566L3.68657 21.1824Z"
        fill="#0569EA"
      />
      <path
        fillRule="evenodd"
        clipRule="evenodd"
        d="M15.8385 0L29.677 7.99336V9.84182L15.8385 17.8352L2 9.84182V7.99336L15.8385 0ZM6.49626 8.61227L15.8275 3.29726L25.2807 8.61227L15.8275 13.9884L6.49626 8.61227Z"
        fill="#0569EA"
      />
    </svg>
  );
};

const FeatureViewIcon16 = () => {
  return <FeatureViewIcon size={16} className="euiSideNavItemButton__icon" />;
};

const FeatureViewIcon32 = () => {
  return (
    <FeatureViewIcon
      size={32}
      className="euiIcon euiIcon--xLarge euiPageHeaderContent__titleIcon"
    />
  );
};

export { FeatureViewIcon, FeatureViewIcon16, FeatureViewIcon32 };
