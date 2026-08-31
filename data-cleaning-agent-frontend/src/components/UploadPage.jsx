import React, { useRef } from "react";
import styled from "styled-components";
import { FaUpload } from "react-icons/fa";

const Wrapper = styled.div`
  min-height: 100vh;
  background: linear-gradient(135deg, #181e41 40%, #3f5efb 100%);
  padding: 2.5rem 0;
`;

const UploadBox = styled.div`
  background: linear-gradient(115deg, #ffe08233 0%, #3f5efb22 100%);
  border: 3px dashed #fc466b;
  border-radius: 36px;
  padding: 3.5rem 2rem;
  text-align: center;
  margin: 2.5rem auto;
  box-shadow: 0 6px 44px #3f5efb33;
  cursor: pointer;
  max-width: 600px;
  transition: border 0.2s, background 0.2s;
  &:hover {
    border: 3px solid #fc466b;
    background: linear-gradient(115deg, #fc466b11 0%, #ffe08233 100%);
  }
`;

const Label = styled.h2`
  color: #3f5efb;
  margin-bottom: 1.4rem;
  font-weight: 800;
  font-size: 2rem;
  letter-spacing: 0.01em;
`;

const FileInput = styled.input`
  display: none;
`;

const Info = styled.p`
  color: #fff;
  font-size: 1.13rem;
  margin-top: 1.5rem;
`;

const NextBtn = styled.button`
  background: linear-gradient(90deg, #fc466b, #ffe082 100%);
  border: none;
  padding: 1rem 2.8rem;
  border-radius: 36px;
  font-size: 1.18rem;
  font-weight: bold;
  color: #3f5efb;
  margin-top: 2.7rem;
  cursor: pointer;
  box-shadow: 0 2px 14px #fc466b35;
  transition: 0.2s;
  &:hover {
    background: linear-gradient(90deg, #3f5efb, #fc466b 100%);
    color: #ffe082;
    box-shadow: 0 4px 16px #3f5efb44;
  }
`;

export default function UploadPage({ setStep, setFileData }) {
  const fileInputRef = useRef();

  function handleFileChange(e) {
    const file = e.target.files[0];
    if (!file) return;
    setFileData({
      name: file.name,
      type: file.type,
      size: file.size,
      url: URL.createObjectURL(file),
      file
    });
  }

  return (
    <Wrapper>
      <UploadBox onClick={() => fileInputRef.current.click()}>
        <FaUpload style={{ fontSize: "3.3rem", color: "#fc466b", marginBottom: "1.2rem" }} />
        <Label>Click or Drag &amp; Drop to Upload Your Data File</Label>
        <Info>
          Supported: <b>.csv</b>, <b>.xlsx</b>, <b>.json</b>
        </Info>
        <FileInput
          ref={fileInputRef}
          type="file"
          accept=".csv,.xlsx,.json"
          onChange={handleFileChange}
        />
      </UploadBox>
      <div style={{ textAlign: "center" }}>
        <NextBtn onClick={() => setStep(2)}>
          Next →
        </NextBtn>
      </div>
    </Wrapper>
  );
}