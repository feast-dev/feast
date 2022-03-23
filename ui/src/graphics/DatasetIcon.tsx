import React from "react";

const DatasetIcon = ({
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
      <path d="M4 8.59285V5.39551H28.4797V8.59285H4Z" fill="#0569EA" />
      <path d="M4 14.5878V11.2906H7.29726V14.5878H4Z" fill="#0569EA" />
      <path
        d="M10.0949 14.5878V11.2906H28.4797V14.5878H10.0949Z"
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

const DatasetIcon16 = () => {
  return <DatasetIcon size={16} className="euiSideNavItemButton__icon" />;
};

const DatasetIcon32 = () => {
  return (
    <DatasetIcon
      size={32}
      className="euiIcon euiIcon--xLarge euiPageHeaderContent__titleIcon"
    />
  );
};

export { DatasetIcon, DatasetIcon16, DatasetIcon32 };
