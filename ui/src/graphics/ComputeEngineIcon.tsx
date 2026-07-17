import React from "react";

const ComputeEngineIcon = (props: React.SVGProps<SVGSVGElement>) => {
  return (
    <svg {...props} viewBox="0 0 32 32" fill="none">
      <path
        d="M4 8C4 6.89543 4.89543 6 6 6H26C27.1046 6 28 6.89543 28 8V12C28 13.1046 27.1046 14 26 14H6C4.89543 14 4 13.1046 4 12V8Z"
        fill="#0569EA"
      />
      <circle cx="8" cy="10" r="1.5" fill="white" />
      <circle cx="12" cy="10" r="1.5" fill="white" />
      <rect
        x="16"
        y="9"
        width="8"
        height="2"
        rx="1"
        fill="white"
        opacity="0.6"
      />
      <path
        d="M4 20C4 18.8954 4.89543 18 6 18H26C27.1046 18 28 18.8954 28 20V24C28 25.1046 27.1046 26 26 26H6C4.89543 26 4 25.1046 4 24V20Z"
        fill="#0569EA"
        opacity="0.6"
      />
      <circle cx="8" cy="22" r="1.5" fill="white" />
      <circle cx="12" cy="22" r="1.5" fill="white" />
      <rect
        x="16"
        y="21"
        width="8"
        height="2"
        rx="1"
        fill="white"
        opacity="0.6"
      />
    </svg>
  );
};

export { ComputeEngineIcon };
