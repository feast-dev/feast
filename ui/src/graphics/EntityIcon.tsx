import React from "react";

const EntityIcon = ({
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
        d="M16.0383 5.04579C9.99584 5.04579 5.09743 9.9442 5.09743 15.9867C5.09743 21.5208 9.20619 26.0952 14.5396 26.8258V21.5321H17.5371C17.5469 22.3595 17.5371 29.946 17.5371 29.946C17.0447 29.9982 16.5446 30.025 16.0383 30.025C8.28518 30.025 2 23.7399 2 15.9867C2 8.23354 8.28518 1.94836 16.0383 1.94836C23.7915 1.94836 30.0767 8.23354 30.0767 15.9867C30.0767 22.0929 26.1782 27.2885 20.7344 29.2203C20.6845 29.0259 20.6967 26.2271 20.7344 25.8713C24.427 24.1139 26.9792 20.3484 26.9792 15.9867C26.9792 9.9442 22.0808 5.04579 16.0383 5.04579Z"
        fill="#0569EA"
      />
      <path
        fillRule="evenodd"
        clipRule="evenodd"
        d="M16.0383 11.4405C13.5275 11.4405 11.4921 13.4759 11.4921 15.9867C11.4921 18.4975 13.5275 20.5329 16.0383 20.5329C18.5491 20.5329 20.5846 18.4975 20.5846 15.9867C20.5846 13.4759 18.5491 11.4405 16.0383 11.4405ZM8.39468 15.9867C8.39468 11.7653 11.8169 8.34308 16.0383 8.34308C20.2598 8.34308 23.682 11.7653 23.682 15.9867C23.682 20.2082 20.2598 23.6304 16.0383 23.6304C11.8169 23.6304 8.39468 20.2082 8.39468 15.9867Z"
        fill="#0569EA"
      />
    </svg>
  );
};

const EntityIcon16 = () => {
  return <EntityIcon size={16} className="euiSideNavItemButton__icon" />;
};

const EntityIcon32 = () => {
  return (
    <EntityIcon
      size={32}
      className="euiIcon euiIcon--xLarge euiPageHeaderContent__titleIcon"
    />
  );
};

export { EntityIcon, EntityIcon16, EntityIcon32 };
