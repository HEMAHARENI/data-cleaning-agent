import React, { useState } from "react";
import styled from "styled-components";
import { FaCog } from "react-icons/fa";

const Wrapper = styled.div`
  min-height: 100vh;
  background: linear-gradient(135deg, #181e41 40%, #3f5efb 100%);
  padding: 2.5rem 0;
`;

const PreBox = styled.div`
  background: rgba(39, 47, 89, 0.7);
  border-radius: 28px;
  padding: 2.5rem 2rem;
  box-shadow: 0 6px 48px 0 #3f5efb50;
  margin: 2.5rem auto 0 auto;
  max-width: 680px;
  border: 1px solid #383d5a;
`;

const SectionTitle = styled.h2`
  font-weight: 800;
  font-size: 2.2rem;
  letter-spacing: 0.01em;
  margin-bottom: 1.2rem;
  color: #ffe082;
  display: flex;
  align-items: center;
  gap: 1.1rem;
`;

const Option = styled.div`
  display: flex;
  align-items: center;
  margin: 1.1rem 0;
  label {
    font-size: 1.13rem;
    color: #e3e5ec;
    margin-left: 1rem;
    font-weight: 600;
    letter-spacing: 0.02em;
  }
  input[type="checkbox"] {
    accent-color: #3f5efb;
    width: 1.25rem;
    height: 1.25rem;
    box-shadow: 0 0 2px #ffe082;
  }
`;

const NextBtn = styled.button`
  background: linear-gradient(90deg, #3f5efb, #ffe082 100%);
  border: none;
  padding: 1rem 2.8rem;
  border-radius: 36px;
  font-size: 1.18rem;
  font-weight: bold;
  color: #fc466b;
  margin-top: 2.7rem;
  cursor: pointer;
  box-shadow: 0 2px 14px #3f5efb35;
  transition: 0.2s;
  &:hover {
    background: linear-gradient(90deg, #fc466b, #ffe082 100%);
    color: #3f5efb;
    box-shadow: 0 4px 16px #fc466b44;
  }
`;

export default function PreprocessPage({ setStep }) {
  const [opts, setOpts] = useState({
    normalize: true,
    oneHot: false,
    labelEncode: false,
    outlier: false
  });

  function toggleOpt(key) {
    setOpts(o => ({ ...o, [key]: !o[key] }));
  }

  return (
    <Wrapper>
      <PreBox>
        <SectionTitle>
          <FaCog />
          Preprocessing Tools
        </SectionTitle>
        <Option>
          <input type="checkbox" checked={opts.normalize} onChange={() => toggleOpt("normalize")} />
          <label>Normalize numeric columns</label>
        </Option>
        <Option>
          <input type="checkbox" checked={opts.oneHot} onChange={() => toggleOpt("oneHot")} />
          <label>One-hot encode categorical</label>
        </Option>
        <Option>
          <input type="checkbox" checked={opts.labelEncode} onChange={() => toggleOpt("labelEncode")} />
          <label>Label encode categorical</label>
        </Option>
        <Option>
          <input type="checkbox" checked={opts.outlier} onChange={() => toggleOpt("outlier")} />
          <label>Remove outliers</label>
        </Option>
        <div style={{ color: "#ffe082", marginTop: "1.8rem", fontWeight: 600, fontSize: "1.07rem" }}>
          Pick as many as you want!<br />
          <span style={{ color: "#fff", fontWeight: 400 }}>Best for ML magic.</span>
        </div>
      </PreBox>
      <NextBtn onClick={() => setStep(5)}>
        Next →
      </NextBtn>
    </Wrapper>
  );
}