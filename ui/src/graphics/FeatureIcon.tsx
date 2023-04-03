import React from "react";

const FeatureIcon = ({
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
        d="M15.8385 0L29.677 7.99336V9.84182L15.8385 17.8352L2 9.84182V7.99336L15.8385 0ZM6.49626 8.61227L15.8275 3.29726L25.2807 8.61227L15.8275 13.9884L6.49626 8.61227Z"
        fill="#0569EA"
      />
      <path d="M4 20.5829V17.2856H7.29726V20.5829H4Z" fill="#0569EA" />
      <path
        d="M10.0949 20.5829V17.2856H28.4797V20.5829H10.0949Z"
        fill="#0569EA"
      />
      <path d="M4 26.5779V23.2806H7.29726V26.5779H4Z" fill="#0569EA" />
      <path
        d="M10.0949 26.5779V23.2806H28.4797V26.5779H10.0949Z"
        fill="#0569EA"
      />
    </svg>
  );
};

const FeatureIcon16 = () => {
  return <FeatureIcon size={16} className="euiSideNavItemButton__icon" />;
};

const FeatureIcon32 = () => {
  return (
    <FeatureIcon
      size={32}
      className="euiIcon euiIcon--xLarge euiPageHeaderContent__titleIcon"
    />
  );
};

export { FeatureIcon, FeatureIcon16, FeatureIcon32 };
